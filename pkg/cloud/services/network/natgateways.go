/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package network

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/pkg/errors"
	kerrors "k8s.io/apimachinery/pkg/util/errors"

	infrav1 "sigs.k8s.io/cluster-api-provider-aws/v2/api/v1beta2"
	"sigs.k8s.io/cluster-api-provider-aws/v2/pkg/cloud/awserrors"
	"sigs.k8s.io/cluster-api-provider-aws/v2/pkg/cloud/converters"
	"sigs.k8s.io/cluster-api-provider-aws/v2/pkg/cloud/filter"
	"sigs.k8s.io/cluster-api-provider-aws/v2/pkg/cloud/services"
	"sigs.k8s.io/cluster-api-provider-aws/v2/pkg/cloud/services/wait"
	"sigs.k8s.io/cluster-api-provider-aws/v2/pkg/cloud/tags"
	"sigs.k8s.io/cluster-api-provider-aws/v2/pkg/record"
	clusterv1beta1 "sigs.k8s.io/cluster-api/api/core/v1beta1"
	v1beta1conditions "sigs.k8s.io/cluster-api/util/deprecated/v1beta1/conditions"
)

func (s *Service) reconcileNatGateways() error {
	if s.scope.VPC().IsUnmanaged(s.scope.Name()) {
		s.scope.Trace("Skipping NAT gateway reconcile in unmanaged mode")
		_, err := s.updateNatGatewayIPs(s.scope.TagUnmanagedNetworkResources())
		if err != nil {
			return err
		}
		return nil
	}

	s.scope.Debug("Reconciling NAT gateways")

	if len(s.scope.Subnets().FilterPrivate().FilterNonCni()) == 0 {
		s.scope.Debug("No private subnets available, skipping NAT gateways")
		v1beta1conditions.MarkFalse(
			s.scope.InfraCluster(),
			infrav1.NatGatewaysReadyCondition,
			infrav1.NatGatewaysReconciliationFailedReason,
			clusterv1beta1.ConditionSeverityWarning,
			"No private subnets available, skipping NAT gateways")
		return nil
	} else if len(s.scope.Subnets().FilterPublic().FilterNonCni()) == 0 {
		s.scope.Debug("No public subnets available. Cannot create NAT gateways for private subnets, this might be a configuration error.")
		v1beta1conditions.MarkFalse(
			s.scope.InfraCluster(),
			infrav1.NatGatewaysReadyCondition,
			infrav1.NatGatewaysReconciliationFailedReason,
			clusterv1beta1.ConditionSeverityWarning,
			"No public subnets available. Cannot create NAT gateways for private subnets, this might be a configuration error.")
		return nil
	}

	subnetIDs, err := s.updateNatGatewayIPs(true)
	if err != nil {
		return err
	}

	// Batch the creation of NAT gateways
	if len(subnetIDs) > 0 {
		// set NatGatewayCreationStarted if the condition has never been set before
		if !v1beta1conditions.Has(s.scope.InfraCluster(), infrav1.NatGatewaysReadyCondition) {
			v1beta1conditions.MarkFalse(s.scope.InfraCluster(), infrav1.NatGatewaysReadyCondition, infrav1.NatGatewaysCreationStartedReason, clusterv1beta1.ConditionSeverityInfo, "")
			if err := s.scope.PatchObject(); err != nil {
				return errors.Wrap(err, "failed to patch conditions")
			}
		}
		ngws, err := s.createNatGateways(subnetIDs)

		subnets := s.scope.Subnets()
		defer func() {
			s.scope.SetSubnets(subnets)
		}()
		for _, ng := range ngws {
			subnet := subnets.FindByID(*ng.SubnetId)
			subnet.NatGatewayID = ng.NatGatewayId
		}

		if err != nil {
			return err
		}
		v1beta1conditions.MarkTrue(s.scope.InfraCluster(), infrav1.NatGatewaysReadyCondition)
	}

	return nil
}

func (s *Service) updateNatGatewayIPs(updateTags bool) ([]string, error) {
	existing, err := s.describeNatGatewaysBySubnet()
	if err != nil {
		return nil, err
	}

	natGatewaysIPs := []string{}
	subnetIDs := []string{}

	// Optional cost-saving mode via VPCSpec.SingleNatGateway.
	// - false / omitted (default): upstream behavior — one NAT per AZ that has private subnets.
	// - true: create/reuse a single NAT shared by all private subnets (see updateNatGatewayIPsSingle).
	// Only applies to managed VPCs; existing NAT layouts are not migrated when the flag changes.
	if s.scope.VPC().SingleNatGateway {
		return s.updateNatGatewayIPsSingle(existing, updateTags)
	}

	// Upstream: Find AZs that have private subnets
	privateSubnetAZs := make(map[string]bool)
	for _, sn := range s.scope.Subnets().FilterPrivate().FilterNonCni() {
		if sn.GetResourceID() != "" {
			privateSubnetAZs[sn.AvailabilityZone] = true
		}
	}

	// Upstream: For each AZ with private subnets, find a public subnet and check for NAT gateway
	processedAZs := make(map[string]bool)
	for _, sn := range s.scope.Subnets().FilterPublic().FilterNonCni() {
		if sn.GetResourceID() == "" {
			continue
		}

		// Only process this AZ if it has private subnets and we haven't processed it yet
		if !privateSubnetAZs[sn.AvailabilityZone] || processedAZs[sn.AvailabilityZone] {
			continue
		}

		// Mark this AZ as processed
		processedAZs[sn.AvailabilityZone] = true

		if ngw, ok := existing[sn.GetResourceID()]; ok {
			if len(ngw.NatGatewayAddresses) > 0 && ngw.NatGatewayAddresses[0].PublicIp != nil {
				natGatewaysIPs = append(natGatewaysIPs, *ngw.NatGatewayAddresses[0].PublicIp)
			}
			if updateTags {
				// Make sure tags are up to date.
				if err := wait.WaitForWithRetryable(wait.NewBackoff(), func() (bool, error) {
					buildParams := s.getNatGatewayTagParams(*ngw.NatGatewayId)
					tagsBuilder := tags.New(&buildParams, tags.WithEC2(s.EC2Client))
					if err := tagsBuilder.Ensure(converters.TagsToMap(ngw.Tags)); err != nil {
						return false, err
					}
					return true, nil
				}, awserrors.ResourceNotFound); err != nil {
					record.Warnf(s.scope.InfraCluster(), "FailedTagNATGateway", "Failed to tag managed NAT Gateway %q: %v", *ngw.NatGatewayId, err)
					return nil, errors.Wrapf(err, "failed to tag nat gateway %q", *ngw.NatGatewayId)
				}
			}

			continue
		}

		subnetIDs = append(subnetIDs, sn.GetResourceID())
	}

	s.scope.SetNatGatewaysIPs(natGatewaysIPs)
	return subnetIDs, nil
}

// updateNatGatewayIPsSingle implements SingleNatGateway mode.
//
// Goal: reduce NAT hourly/EIP cost by provisioning one NAT for the whole VPC
// instead of one per AZ. Tradeoffs: private egress loses AZ HA (the NAT's AZ
// is a SPOF) and cross-AZ data to the NAT may incur transfer charges.
//
// Placement: prefer any already-existing NAT on a public NonCNI subnet; otherwise
// create exactly one NAT in the lexicographically first public NonCNI subnet
// (sorted by AZ, then subnet ID) so placement is stable across reconciles.
// Private route tables are wired to that NAT later via getNatGatewayForSubnet.
//
// Contract with reconcileNatGateways / createNatGateways:
//   - return value subnetIDs lists public subnet IDs that still need a NAT created.
//     In this mode that list is either empty (NAT already present) or length 1.
//   - natGatewaysIPs is persisted on the scope for status / callers that expose
//     the shared NAT public IP; it is empty when creating for the first time
//     (IP is unknown until CreateNatGateway completes on a later reconcile).
//
// existing is keyed by the public subnet ID where each NAT currently lives
// (from describeNatGatewaysBySubnet). We never intentionally create a second NAT
// while one already exists in this map for any public subnet we own.
func (s *Service) updateNatGatewayIPsSingle(existing map[string]types.NatGateway, updateTags bool) ([]string, error) {
	// Accumulator for public IPs of NATs we already have (usually 0 or 1 entry).
	natGatewaysIPs := []string{}
	// Subnet IDs where createNatGateways should place a new NAT; stay empty if we reuse.
	subnetIDs := []string{}

	// Same gate as upstream: with no private NonCNI subnets there is nothing to NAT for.
	// Skip allocating a shared NAT (and EIP) that would sit unused.
	if len(s.scope.Subnets().FilterPrivate().FilterNonCni()) == 0 {
		// Still write an empty IP list so stale status from a previous layout is cleared.
		s.scope.SetNatGatewaysIPs(natGatewaysIPs)
		return subnetIDs, nil
	}

	// Candidates for hosting the shared NAT: public + NonCNI only.
	// CNI / secondary ENI-style subnets are excluded so we do not place NAT where
	// route-table ownership is atypical.
	publicSubnets := s.scope.Subnets().FilterPublic().FilterNonCni()

	// Deterministic order across reconciles and controller replicas.
	// Primary key: AvailabilityZone (string sort, e.g. us-east-1a before us-east-1b).
	// Secondary key: subnet resource ID, so multiple publics in one AZ stay stable.
	// Without this, map/slice iteration order could flip createOnSubnetID between runs
	// and churn NAT placement if we were always creating (reuse path avoids that once present).
	sort.SliceStable(publicSubnets, func(i, j int) bool {
		if publicSubnets[i].AvailabilityZone == publicSubnets[j].AvailabilityZone {
			return publicSubnets[i].GetResourceID() < publicSubnets[j].GetResourceID()
		}
		return publicSubnets[i].AvailabilityZone < publicSubnets[j].AvailabilityZone
	})

	// Walk sorted public subnets once:
	//  1) Prefer the first subnet that already has a NAT in `existing` → reuse path.
	//  2) Else remember the first subnet that has a resource ID as createOnSubnetID.
	// Subnets without an ID yet (not created in AWS) are skipped for both reuse and create.
	var existingNGW *types.NatGateway
	createOnSubnetID := ""
	for _, sn := range publicSubnets {
		if sn.GetResourceID() == "" {
			// Spec-only stub; cannot attach or look up a NAT until EC2 has a subnet ID.
			continue
		}
		if ngw, ok := existing[sn.GetResourceID()]; ok {
			// Found a live NAT on this public subnet. Take the first hit in sorted order
			// and stop: we deliberately ignore additional NATs (e.g. leftover from a prior
			// per-AZ layout). Cleanup of extras is outside this function's scope.
			existingNGW = &ngw
			break
		}
		if createOnSubnetID == "" {
			// First usable public subnet in sort order becomes the create target if
			// no existing NAT was found later in the loop (we only set this once).
			createOnSubnetID = sn.GetResourceID()
		}
	}

	// --- Reuse path: a shared NAT already exists somewhere in the VPC ---
	if existingNGW != nil {
		// Record the Elastic IP / public address for cluster status when AWS has attached one.
		// Pending NATs may briefly have empty NatGatewayAddresses; IPs update on next reconcile.
		if len(existingNGW.NatGatewayAddresses) > 0 && existingNGW.NatGatewayAddresses[0].PublicIp != nil {
			natGatewaysIPs = append(natGatewaysIPs, *existingNGW.NatGatewayAddresses[0].PublicIp)
		}
		// Tag sync mirrors the per-AZ updateNatGatewayIPs branch: keep cluster/role Name tags
		// current without recreating the gateway. Skipped when updateTags is false (e.g. hot paths).
		if updateTags {
			if err := wait.WaitForWithRetryable(wait.NewBackoff(), func() (bool, error) {
				buildParams := s.getNatGatewayTagParams(*existingNGW.NatGatewayId)
				tagsBuilder := tags.New(&buildParams, tags.WithEC2(s.EC2Client))
				if err := tagsBuilder.Ensure(converters.TagsToMap(existingNGW.Tags)); err != nil {
					return false, err
				}
				return true, nil
			}, awserrors.ResourceNotFound); err != nil {
				record.Warnf(s.scope.InfraCluster(), "FailedTagNATGateway", "Failed to tag managed NAT Gateway %q: %v", *existingNGW.NatGatewayId, err)
				return nil, errors.Wrapf(err, "failed to tag nat gateway %q", *existingNGW.NatGatewayId)
			}
		}
		s.scope.SetNatGatewaysIPs(natGatewaysIPs)
		// Empty subnetIDs is the signal to the caller: do not call CreateNatGateway again.
		// Private subnets in other AZs will still get 0.0.0.0/0 → this NAT via getNatGatewayForSubnet.
		return subnetIDs, nil
	}

	// --- Create path: no NAT in `existing` for any public subnet we scanned ---
	if createOnSubnetID != "" {
		s.scope.Info("Using single shared NAT gateway for all private subnets", "subnet-id", createOnSubnetID)
		// Exactly one subnet ID → createNatGateways allocates one EIP and creates one NAT
		// in that public subnet. Subsequent reconciles hit the reuse path above.
		subnetIDs = append(subnetIDs, createOnSubnetID)
	}
	// If createOnSubnetID stayed empty, every public subnet lacked a resource ID (or there
	// were none): return empty subnetIDs and let subnet reconcile catch up first.

	s.scope.SetNatGatewaysIPs(natGatewaysIPs)
	return subnetIDs, nil
}

func (s *Service) deleteNatGateways() error {
	if s.scope.VPC().IsUnmanaged(s.scope.Name()) {
		s.scope.Trace("Skipping NAT gateway deletion in unmanaged mode")
		return nil
	}

	if len(s.scope.Subnets().FilterPrivate()) == 0 {
		s.scope.Debug("No private subnets available, skipping NAT gateways")
		return nil
	} else if len(s.scope.Subnets().FilterPublic()) == 0 {
		s.scope.Debug("No public subnets available. Cannot create NAT gateways for private subnets, this might be a configuration error.")
		return nil
	}

	existing, err := s.describeNatGatewaysBySubnet()
	if err != nil {
		return err
	}

	var ngIDs []types.NatGateway
	for _, sn := range s.scope.Subnets().FilterPublic() {
		if sn.GetResourceID() == "" {
			continue
		}

		if ngID, ok := existing[sn.GetResourceID()]; ok {
			ngIDs = append(ngIDs, ngID)
		}
	}

	c := make(chan error, len(ngIDs))
	errs := []error{}

	for _, ngID := range ngIDs {
		go func(c chan error, ngID types.NatGateway) {
			err := s.deleteNatGateway(*ngID.NatGatewayId)
			c <- err
		}(c, ngID)
	}

	for range ngIDs {
		ngwErr := <-c
		if ngwErr != nil {
			errs = append(errs, ngwErr)
		}
	}

	return kerrors.NewAggregate(errs)
}

func (s *Service) describeNatGatewaysBySubnet() (map[string]types.NatGateway, error) {
	describeNatGatewayInput := &ec2.DescribeNatGatewaysInput{
		Filter: []types.Filter{
			filter.EC2.VPC(s.scope.VPC().ID),
			filter.EC2.NATGatewayStates(types.NatGatewayStatePending, types.NatGatewayStateAvailable),
		},
	}

	gateways := make(map[string]types.NatGateway)

	paginator := ec2.NewDescribeNatGatewaysPaginator(s.EC2Client, describeNatGatewayInput)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(context.TODO())
		if err != nil {
			record.Eventf(s.scope.InfraCluster(), "FailedDescribeNATGateways", "Failed to describe NAT gateways with VPC ID %q: %v", s.scope.VPC().ID, err)
			return nil, errors.Wrapf(err, "failed to describe NAT gateways with VPC ID %q", s.scope.VPC().ID)
		}
		for _, r := range output.NatGateways {
			gateways[aws.ToString(r.SubnetId)] = r
		}
	}

	return gateways, nil
}

func (s *Service) getNatGatewayTagParams(id string) infrav1.BuildParams {
	name := fmt.Sprintf("%s-nat", s.scope.Name())

	return infrav1.BuildParams{
		ClusterName: s.scope.Name(),
		ResourceID:  id,
		Lifecycle:   infrav1.ResourceLifecycleOwned,
		Name:        aws.String(name),
		Role:        aws.String(infrav1.CommonRoleTagValue),
		Additional:  s.scope.AdditionalTags(),
	}
}

func (s *Service) createNatGateways(subnetIDs []string) ([]*types.NatGateway, error) {
	eips, err := s.getOrAllocateAddresses(len(subnetIDs), infrav1.CommonRoleTagValue, s.scope.VPC().GetElasticIPPool())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create one or more IP addresses for NAT gateways")
	}

	natgateways := make([]*types.NatGateway, 0, len(subnetIDs))
	type ngwCreation struct {
		natGateway *types.NatGateway
		error      error
	}
	c := make(chan ngwCreation, len(subnetIDs))

	for i, sn := range subnetIDs {
		go func(c chan ngwCreation, subnetID, ip string) {
			ngw, err := s.createNatGateway(subnetID, ip)
			c <- ngwCreation{natGateway: ngw, error: err}
		}(c, sn, eips[i])
	}

	for range subnetIDs {
		ngwResult := <-c
		if ngwResult.error != nil {
			return nil, ngwResult.error
		}
		natgateways = append(natgateways, ngwResult.natGateway)
	}
	return natgateways, nil
}

func (s *Service) createNatGateway(subnetID, ip string) (*types.NatGateway, error) {
	var out *ec2.CreateNatGatewayOutput
	var err error

	if err := wait.WaitForWithRetryable(wait.NewBackoff(), func() (bool, error) {
		if out, err = s.EC2Client.CreateNatGateway(context.TODO(), &ec2.CreateNatGatewayInput{
			SubnetId:          aws.String(subnetID),
			AllocationId:      aws.String(ip),
			TagSpecifications: []types.TagSpecification{tags.BuildParamsToTagSpecification(types.ResourceTypeNatgateway, s.getNatGatewayTagParams(services.TemporaryResourceID))},
		}); err != nil {
			return false, err
		}
		return true, nil
	}, awserrors.InvalidSubnet); err != nil {
		record.Warnf(s.scope.InfraCluster(), "FailedCreateNATGateway", "Failed to create new NAT Gateway: %v", err)
		return nil, errors.Wrapf(err, "failed to create NAT gateway for subnet ID %q", subnetID)
	}
	record.Eventf(s.scope.InfraCluster(), "SuccessfulCreateNATGateway", "Created new NAT Gateway %q", *out.NatGateway.NatGatewayId)

	if err := ec2.NewNatGatewayAvailableWaiter(s.EC2Client).Wait(context.TODO(), &ec2.DescribeNatGatewaysInput{
		NatGatewayIds: []string{aws.ToString(out.NatGateway.NatGatewayId)},
	}, time.Minute*2); err != nil {
		return nil, errors.Wrapf(err, "failed to wait for nat gateway %q in subnet %q", *out.NatGateway.NatGatewayId, subnetID)
	}

	s.scope.Info("Created NAT gateway for subnet", "nat-gateway-id", *out.NatGateway.NatGatewayId, "subnet-id", subnetID)
	return out.NatGateway, nil
}

func (s *Service) deleteNatGateway(id string) error {
	_, err := s.EC2Client.DeleteNatGateway(context.TODO(), &ec2.DeleteNatGatewayInput{
		NatGatewayId: aws.String(id),
	})
	if err != nil {
		record.Warnf(s.scope.InfraCluster(), "FailedDeleteNATGateway", "Failed to delete NAT Gateway %q previously attached to VPC %q: %v", id, s.scope.VPC().ID, err)
		return errors.Wrapf(err, "failed to delete nat gateway %q", id)
	}
	record.Eventf(s.scope.InfraCluster(), "SuccessfulDeleteNATGateway", "Deleted NAT Gateway %q previously attached to VPC %q", id, s.scope.VPC().ID)
	s.scope.Info("Deleted NAT gateway in VPC", "nat-gateway-id", id, "vpc-id", s.scope.VPC().ID)

	describeInput := &ec2.DescribeNatGatewaysInput{
		NatGatewayIds: []string{id},
	}

	if err := wait.WaitForWithRetryable(wait.NewBackoff(), func() (done bool, err error) {
		out, err := s.EC2Client.DescribeNatGateways(context.TODO(), describeInput)
		if err != nil {
			return false, err
		}

		if out == nil || len(out.NatGateways) == 0 {
			return false, fmt.Errorf("no NAT gateway returned for id %q", id)
		}

		ng := out.NatGateways[0]
		switch state := ng.State; state {
		case types.NatGatewayStateAvailable, types.NatGatewayStateDeleting:
			return false, nil
		case types.NatGatewayStateDeleted:
			return true, nil
		case types.NatGatewayStatePending:
			return false, errors.Errorf("in pending state")
		case types.NatGatewayStateFailed:
			return false, errors.Errorf("in failed state: %q - %s", *ng.FailureCode, *ng.FailureMessage)
		}

		return false, errors.Errorf("in unknown state")
	}); err != nil {
		return errors.Wrapf(err, "failed to wait for NAT gateway deletion %q", id)
	}

	return nil
}

// getNatGatewayForSubnet returns the NAT gateway ID to use for a private subnet's
// default route (0.0.0.0/0). Selection order:
//
//  1. Same-AZ public NAT (always preferred when present).
//  2. Parent-zone NAT (edge / Local / Wavelength zones).
//  3. Cross-AZ fallback to any available public NAT — only when:
//     - the subnet is an edge zone (upstream behavior; edge AZs often have no local NAT), or
//     - VPCSpec.SingleNatGateway is true (all private RTs share one VPC NAT).
//
// When SingleNatGateway is false/omitted, a regular (non-edge) private subnet with
// no NAT in its AZ returns an error — matching upstream HA assumptions.
func (s *Service) getNatGatewayForSubnet(sn *infrav1.SubnetSpec) (string, error) {
	if sn.IsPublic {
		return "", errors.Errorf("cannot get NAT gateway for a public subnet, got id %q", sn.GetResourceID())
	}

	azGateways := make(map[string]string)
	azNames := []string{}
	for _, psn := range s.scope.Subnets().FilterPublic() {
		if psn.NatGatewayID == nil {
			continue
		}
		if _, ok := azGateways[psn.AvailabilityZone]; !ok {
			azGateways[psn.AvailabilityZone] = *psn.NatGatewayID
			azNames = append(azNames, psn.AvailabilityZone)
		}
	}

	if gws, ok := azGateways[sn.AvailabilityZone]; ok && len(gws) > 0 {
		return gws, nil
	}

	// Prefer parent-zone NAT for edge zones when available.
	if sn.ParentZoneName != nil {
		if gws, ok := azGateways[aws.ToString(sn.ParentZoneName)]; ok && len(gws) > 0 {
			return gws, nil
		}
	}

	// SingleNatGateway extends cross-AZ NAT reuse to regular AZs as well as edge.
	allowCrossAZFallback := sn.IsEdge() || s.scope.VPC().SingleNatGateway
	if allowCrossAZFallback {
		sort.Strings(azNames)
		for _, zone := range azNames {
			gw := azGateways[zone]
			if len(gw) > 0 {
				s.scope.Debug("Assigning shared NAT gateway", "nat-gateway-id", gw, "source-zone", zone, "target-zone", sn.AvailabilityZone)
				return gw, nil
			}
		}
	}

	if sn.IsEdge() {
		return "", errors.Errorf("no nat gateways available in %q for private edge subnet %q, current state: %+v", sn.AvailabilityZone, sn.GetResourceID(), azGateways)
	}
	return "", errors.Errorf("no nat gateways available in %q for private subnet %q", sn.AvailabilityZone, sn.GetResourceID())
}
