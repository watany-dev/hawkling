package aws

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/schollz/progressbar/v3"
)

// For testing
var testClient IAMClient

// SetTestClient sets a test client for unit testing
func SetTestClient(client IAMClient) {
	testClient = client
}

// ClearTestClient clears the test client after tests
func ClearTestClient() {
	testClient = nil
}

// AWSclient implements the IAMClient interface
type AWSClient struct {
	iamClient *iam.Client
}

// NewAWSClient creates a new AWS client with the specified profile and region
func NewAWSClient(ctx context.Context, profile, region string) (IAMClient, error) {
	// If we're in test mode, return the test client
	if testClient != nil {
		return testClient, nil
	}

	var cfg aws.Config
	var err error

	if profile != "" {
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(region),
			config.WithSharedConfigProfile(profile),
		)
	} else {
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(region),
		)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return &AWSClient{
		iamClient: iam.NewFromConfig(cfg),
	}, nil
}

// ListRoles returns all IAM roles
func (c *AWSClient) ListRoles(ctx context.Context) ([]Role, error) {
	// Pre-allocate roles slice to reduce allocations
	roles := make([]Role, 0, 100) // Start with a reasonable capacity

	// Get all roles
	paginator := iam.NewListRolesPaginator(c.iamClient, &iam.ListRolesInput{})
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list roles: %w", err)
		}

		// Convert the AWS roles to our Role type
		for _, r := range output.Roles {
			role := Role{
				Name:       *r.RoleName,
				Arn:        *r.Arn,
				CreateDate: *r.CreateDate,
			}

			if r.Description != nil {
				role.Description = *r.Description
			}

			// We'll get last used info separately
			roles = append(roles, role)
		}
	}

	// Nothing to hydrate; skip the progress bar and worker pool entirely.
	if len(roles) == 0 {
		return roles, nil
	}

	// Create progress bar
	bar := progressbar.NewOptions(len(roles),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetDescription("[cyan]Fetching role usage data...[reset]"),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprintf(os.Stderr, "\n")
		}),
	)

	// Hydrate each role's last-used timestamp concurrently with a bounded
	// worker pool that writes results directly into the roles slice.
	hydrateLastUsed(ctx, roles, defaultListConcurrency, c.GetRoleLastUsed, func() {
		if err := bar.Add(1); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to update progress bar: %v\n", err)
		}
	})

	return roles, nil
}

// GetRoleLastUsed returns the last used timestamp for a role
func (c *AWSClient) GetRoleLastUsed(ctx context.Context, roleName string) (*time.Time, error) {
	resp, err := c.iamClient.GetRole(ctx, &iam.GetRoleInput{
		RoleName: aws.String(roleName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get role %s: %w", roleName, err)
	}

	if resp.Role.RoleLastUsed.LastUsedDate == nil {
		return nil, nil // Role never used
	}

	return resp.Role.RoleLastUsed.LastUsedDate, nil
}

// DeleteRole deletes an IAM role
func (c *AWSClient) DeleteRole(ctx context.Context, roleName string) error {
	// First detach all policies
	if err := c.DetachRolePolicies(ctx, roleName); err != nil {
		return err
	}

	// Delete all inline policies
	if err := c.DeleteInlinePolicies(ctx, roleName); err != nil {
		return err
	}

	// Delete the role
	_, err := c.iamClient.DeleteRole(ctx, &iam.DeleteRoleInput{
		RoleName: aws.String(roleName),
	})
	if err != nil {
		return fmt.Errorf("failed to delete role %s: %w", roleName, err)
	}

	return nil
}

// DetachRolePolicies detaches all managed policies from a role
func (c *AWSClient) DetachRolePolicies(ctx context.Context, roleName string) error {
	paginator := iam.NewListAttachedRolePoliciesPaginator(c.iamClient, &iam.ListAttachedRolePoliciesInput{
		RoleName: aws.String(roleName),
	})

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list attached policies for role %s: %w", roleName, err)
		}

		for _, policy := range output.AttachedPolicies {
			_, err := c.iamClient.DetachRolePolicy(ctx, &iam.DetachRolePolicyInput{
				RoleName:  aws.String(roleName),
				PolicyArn: policy.PolicyArn,
			})
			if err != nil {
				return fmt.Errorf("failed to detach policy %s from role %s: %w", *policy.PolicyArn, roleName, err)
			}
		}
	}

	return nil
}

// DeleteInlinePolicies deletes all inline policies from a role
func (c *AWSClient) DeleteInlinePolicies(ctx context.Context, roleName string) error {
	paginator := iam.NewListRolePoliciesPaginator(c.iamClient, &iam.ListRolePoliciesInput{
		RoleName: aws.String(roleName),
	})

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list inline policies for role %s: %w", roleName, err)
		}

		for _, policyName := range output.PolicyNames {
			_, err := c.iamClient.DeleteRolePolicy(ctx, &iam.DeleteRolePolicyInput{
				RoleName:   aws.String(roleName),
				PolicyName: aws.String(policyName),
			})
			if err != nil {
				return fmt.Errorf("failed to delete inline policy %s from role %s: %w", policyName, roleName, err)
			}
		}
	}

	return nil
}
