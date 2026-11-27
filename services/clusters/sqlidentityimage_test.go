package clusters

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/internal/extensions"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/shared"
)

const (
	testDigest  = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	testImageWL = "registry.example.com/pg/cnpg-postgres-plus:17.7-wl1@" + testDigest
	testImageR  = "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.7"

	// Images of a repository named postgres.
	testImagePlainPG17 = "registry.example.com/postgres:17.7@" + testDigest
	testImagePlainPG18 = "registry.example.com/postgres:18.7@" + testDigest
)

// testSQLIdentityImages maps the majors of the test images to themselves,
// so tests that do not set a map keep their image.
var testSQLIdentityImages = map[string]string{
	"16": "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:16.3",
	"17": testImage,
	"18": testImagePG18,
}

func withSQLIdentityImages(images map[string]string) testServiceOption {
	return func(c *testServiceConfig) {
		c.sqlIdentityImages = images
	}
}

func requireNeutralError(t *testing.T, err error, code codes.Code) {
	t.Helper()
	require.Equal(t, code, status.Code(err), "error: %v", err)
	msg := strings.ToLower(status.Convert(err).Message())
	for _, s := range []string{"xata", "ghcr", "registry.example.com"} {
		require.NotContains(t, msg, s)
	}
	// Projects shows the message of a refused image to the user.
	require.Equal(t, code == codes.FailedPrecondition, IsImageUnavailableError(err), "error: %v", err)
}

func TestValidateSQLIdentityImages(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		profile string
		images  map[string]string
		wantErr string
	}{
		"legacy without images": {},
		"legacy with images": {
			images: map[string]string{"17": testImageWL},
		},
		"white label with a digest pinned image": {
			profile: "WhiteLabel",
			images:  map[string]string{"17": testImageWL},
		},
		"white label with a tag and a registry port": {
			profile: "WhiteLabel",
			images: map[string]string{
				"17": "localhost:5000/cnpg-postgres-plus:17.7-wl1",
				"18": "localhost:5000/cnpg-postgres-plus:18.7",
			},
		},
		"white label with postgres images": {
			profile: "WhiteLabel",
			images:  map[string]string{"17": testImagePlainPG17, "18": testImagePlainPG18},
		},
		"white label without images": {
			profile: "WhiteLabel",
			wantErr: `sql identity images are required with the sql identity profile "WhiteLabel"`,
		},
		"non numeric key": {
			images:  map[string]string{"pg17": testImageWL},
			wantErr: `key "pg17" is not a Postgres major version`,
		},
		"non canonical key": {
			images:  map[string]string{"017": testImageWL},
			wantErr: `key "017" is not a Postgres major version`,
		},
		"major mismatch": {
			images:  map[string]string{"18": testImageWL},
			wantErr: "image for 18 has major version 17",
		},
		"digest without tag": {
			images:  map[string]string{"17": "registry.example.com/cnpg-postgres-plus@" + testDigest},
			wantErr: "image for 17 has no tag",
		},
		"malformed digest": {
			images:  map[string]string{"17": "registry.example.com/cnpg-postgres-plus:17.7@sha256:abc"},
			wantErr: "image for 17",
		},
		"invalid reference": {
			images:  map[string]string{"17": "Registry/Cnpg:17.7"},
			wantErr: "image for 17",
		},
		"major only tag": {
			images:  map[string]string{"17": "registry.example.com/cnpg-postgres-plus:17"},
			wantErr: "image for 17: invalid version format",
		},
		"unknown repository": {
			images:  map[string]string{"17": "registry.example.com/pg/postgres-custom:17.7"},
			wantErr: "image for 17 has no extension list",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg := Config{
				ClustersStorageClass:        "xatastor",
				ClustersVolumeSnapshotClass: "xatastor",
				CloudProvider:               CloudProviderAWS,
				SQLIdentityProfile:          tc.profile,
				SQLIdentityImages:           tc.images,
			}
			err := cfg.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// A tag suffix and a digest do not change the extension list of an image.
func TestPrivilegedExtensionsTagSuffix(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ image, regular string }{
		{testImageWL, testImageR},
		{"registry.example.com/pg/cnpg-postgres-plus:18.7-wl1@" + testDigest, testImagePG18},
		{testImagePlainPG17, testImageR},
		{testImagePlainPG18, testImagePG18},
	} {
		t.Run(tc.image, func(t *testing.T) {
			t.Parallel()

			require.NotEmpty(t, extensions.GetExtensions("postgres:18.7-wl1"))
			require.Equal(t, extensions.GetExtensions("postgres:18.1"), extensions.GetExtensions("postgres:18.7-wl1"))
			for _, id := range []shared.SQLIdentity{shared.LegacySQLIdentity(), shared.WhiteLabelSQLIdentity()} {
				got := privilegedExtensions(tc.image, id)
				require.NotEmpty(t, got)
				require.Equal(t, privilegedExtensions(tc.regular, id), got)
			}
		})
	}
}

func TestCreatePostgresClusterSQLIdentityImage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	images := map[string]string{"17": testImageWL}
	wlParent := func(opts ...parentBranchOption) *v1alpha1.Branch {
		return parentBranch(append([]parentBranchOption{
			withParentSQLIdentity(v1alpha1.SQLIdentityWhiteLabel),
			func(b *v1alpha1.Branch) { b.Spec.ClusterSpec.Image = testImageWL },
		}, opts...)...)
	}

	tests := map[string]struct {
		cellProfile v1alpha1.SQLIdentityProfile
		images      map[string]string // defaults to images
		reqImage    string
		parent      *v1alpha1.Branch
		dataSource  string // "snapshot", "continuous-backup" or "continuous-backup-no-parent"
		wantImage   string
		wantCode    codes.Code
	}{
		"legacy root keeps the requested image": {
			reqImage:  testImage,
			wantImage: testImage,
		},
		"white label root takes the mapped image": {
			cellProfile: v1alpha1.SQLIdentityWhiteLabel,
			reqImage:    testImageR,
			wantImage:   testImageWL,
		},
		"white label root takes the mapped postgres image for 17": {
			cellProfile: v1alpha1.SQLIdentityWhiteLabel,
			images:      map[string]string{"17": testImagePlainPG17, "18": testImagePlainPG18},
			reqImage:    testImageR,
			wantImage:   testImagePlainPG17,
		},
		"white label root takes the mapped postgres image for 18": {
			cellProfile: v1alpha1.SQLIdentityWhiteLabel,
			images:      map[string]string{"17": testImagePlainPG17, "18": testImagePlainPG18},
			reqImage:    testImagePG18,
			wantImage:   testImagePlainPG18,
		},
		"white label root without a mapping is refused": {
			cellProfile: v1alpha1.SQLIdentityWhiteLabel,
			reqImage:    "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:18.1",
			wantCode:    codes.FailedPrecondition,
		},
		"white label root of another offering is refused": {
			cellProfile: v1alpha1.SQLIdentityWhiteLabel,
			reqImage:    "ghcr.io/xataio/postgres-images/xata-analytics:17.7",
			wantCode:    codes.FailedPrecondition,
		},
		"white label root with an unreadable image is refused": {
			cellProfile: v1alpha1.SQLIdentityWhiteLabel,
			reqImage:    "ghcr.io/xataio/postgres-images/cnpg-postgres-plus",
			wantCode:    codes.InvalidArgument,
		},
		"white label snapshot child keeps the parent image": {
			cellProfile: v1alpha1.SQLIdentityWhiteLabel,
			reqImage:    testImageR,
			parent:      wlParent(),
			dataSource:  "snapshot",
			wantImage:   testImageWL,
		},
		"white label xvol clone keeps the parent image": {
			cellProfile: v1alpha1.SQLIdentityWhiteLabel,
			reqImage:    testImageR,
			parent:      wlParent(withWakeupPool("test-pool")),
			dataSource:  "snapshot",
			wantImage:   testImageWL,
		},
		"white label child in a legacy cell keeps the parent image": {
			reqImage:   testImageR,
			parent:     wlParent(),
			dataSource: "snapshot",
			wantImage:  testImageWL,
		},
		"legacy child in a white label cell keeps the parent image": {
			cellProfile: v1alpha1.SQLIdentityWhiteLabel,
			reqImage:    testImageR,
			parent:      parentBranch(),
			dataSource:  "snapshot",
			wantImage:   "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:16.3",
		},
		"white label restore with a parent keeps the source image": {
			cellProfile: v1alpha1.SQLIdentityWhiteLabel,
			reqImage:    testImageR,
			parent:      wlParent(),
			dataSource:  "continuous-backup",
			wantImage:   testImageWL,
		},
		"white label restore without a parent keeps the source image": {
			reqImage:   "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:18.1",
			parent:     wlParent(),
			dataSource: "continuous-backup-no-parent",
			wantImage:  testImageWL,
		},
		"legacy restore without a parent keeps the requested image": {
			cellProfile: v1alpha1.SQLIdentityWhiteLabel,
			reqImage:    testImageR,
			parent:      parentBranch(),
			dataSource:  "continuous-backup-no-parent",
			wantImage:   testImageR,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cellImages := images
			if tc.images != nil {
				cellImages = tc.images
			}
			opts := []testServiceOption{
				withSQLIdentityProfile(tc.cellProfile),
				withSQLIdentityImages(cellImages),
			}
			req, _, _, _, _ := exampleRequestsAndBranches()
			req.Configuration.ImageName = tc.reqImage
			if tc.parent != nil {
				opts = append(opts, withExistingObjects(tc.parent))
				switch tc.dataSource {
				case "snapshot":
					req.ParentId = new(tc.parent.Name)
					req.DataSource = &clustersv1.CreatePostgresClusterRequest_ClusterSnapshot{
						ClusterSnapshot: &clustersv1.ClusterSnapshot{ClusterId: tc.parent.Name},
					}
				case "continuous-backup":
					req.ParentId = new(tc.parent.Name)
					fallthrough
				case "continuous-backup-no-parent":
					req.DataSource = &clustersv1.CreatePostgresClusterRequest_ContinuousBackup{
						ContinuousBackup: &clustersv1.ContinuousBackup{ClusterId: tc.parent.Name},
					}
				}
			}
			svc, k8sClient := setupTestClustersService(t, opts...)

			_, err := svc.CreatePostgresCluster(ctx, req)
			if tc.wantCode != codes.OK {
				requireNeutralError(t, err, tc.wantCode)
				_, err := getBranchFromK8s(ctx, k8sClient, req.GetId())
				require.True(t, apierrors.IsNotFound(err), "branch must not exist: %v", err)
				err = k8sClient.Get(ctx, client.ObjectKey{Name: req.GetId() + "-app", Namespace: "xata-clusters"}, &corev1.Secret{})
				require.True(t, apierrors.IsNotFound(err), "app secret must not exist: %v", err)
				return
			}
			require.NoError(t, err)

			branch, err := getBranchFromK8s(ctx, k8sClient, req.GetId())
			require.NoError(t, err)
			require.Equal(t, tc.wantImage, branch.Spec.ClusterSpec.Image)

			// The parameters follow the final image.
			id := shared.BranchSQLIdentity(branch)
			params := paramsMap(branch.Spec.ClusterSpec.Postgres.Parameters)
			ext := params[id.UtilsSettingPrefix()+"privileged_extensions"]
			require.NotEmpty(t, ext)
			require.Equal(t, privilegedExtensions(tc.wantImage, id), ext)
		})
	}
}

// Legacy Branches get the same spec whether or not the cell has images.
func TestSQLIdentityImagesLegacyGolden(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	run := func(t *testing.T, images map[string]string) (*v1alpha1.Branch, *v1alpha1.Branch, *v1alpha1.Branch) {
		t.Helper()
		parent := parentBranch()
		svc, k8sClient := setupTestClustersService(t,
			withSQLIdentityProfile(v1alpha1.SQLIdentityLegacy),
			withSQLIdentityImages(images),
			withExistingObjects(parent))

		root, _, _, _, _ := exampleRequestsAndBranches()
		_, err := svc.CreatePostgresCluster(ctx, root)
		require.NoError(t, err)

		child, _, _, _, _ := exampleRequestsAndBranches()
		child.Id = "child00000000000000000000a"
		child.ParentId = new(parent.Name)
		child.DataSource = &clustersv1.CreatePostgresClusterRequest_ClusterSnapshot{
			ClusterSnapshot: &clustersv1.ClusterSnapshot{ClusterId: parent.Name},
		}
		_, err = svc.CreatePostgresCluster(ctx, child)
		require.NoError(t, err)

		_, err = svc.UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
			Id:                  parent.Name,
			UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{ImageName: new(testImageR)},
		})
		require.NoError(t, err)

		var out []*v1alpha1.Branch
		for _, id := range []string{root.Id, child.Id, parent.Name} {
			b, err := getBranchFromK8s(ctx, k8sClient, id)
			require.NoError(t, err)
			b.ResourceVersion = ""
			out = append(out, b)
		}
		return out[0], out[1], out[2]
	}

	root, child, updated := run(t, nil)
	require.Equal(t, testImage, root.Spec.ClusterSpec.Image)
	require.Equal(t, "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:16.3", child.Spec.ClusterSpec.Image)
	require.Equal(t, testImageR, updated.Spec.ClusterSpec.Image)

	wlRoot, wlChild, wlUpdated := run(t, map[string]string{"16": testImageWL, "17": testImageWL})
	require.Equal(t, root, wlRoot)
	require.Equal(t, child, wlChild)
	require.Equal(t, updated, wlUpdated)
}

func TestUpdatePostgresClusterSQLIdentityImage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	wlImage18 := "registry.example.com/pg/cnpg-postgres-plus:18.7-wl1@" + testDigest
	images := map[string]string{"17": testImageWL, "18": wlImage18}

	tests := map[string]struct {
		stored    v1alpha1.SQLIdentityProfile
		images    map[string]string
		update    *string
		wantImage string
		wantCode  codes.Code
	}{
		"legacy takes the requested image": {
			images:    images,
			update:    new(testImageR),
			wantImage: testImageR,
		},
		"white label maps the requested image": {
			stored:    v1alpha1.SQLIdentityWhiteLabel,
			images:    images,
			update:    new("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.9"),
			wantImage: testImageWL,
		},
		"white label moves to a new mapped image": {
			stored:    v1alpha1.SQLIdentityWhiteLabel,
			images:    map[string]string{"17": "registry.example.com/pg/cnpg-postgres-plus:17.8-wl2"},
			update:    new(testImageR),
			wantImage: "registry.example.com/pg/cnpg-postgres-plus:17.8-wl2",
		},
		"white label does not move to an older minor version": {
			stored:   v1alpha1.SQLIdentityWhiteLabel,
			images:   map[string]string{"17": "registry.example.com/pg/cnpg-postgres-plus:17.6-wl1"},
			update:   new(testImageR),
			wantCode: codes.FailedPrecondition,
		},
		"white label without a mapping is refused": {
			stored:   v1alpha1.SQLIdentityWhiteLabel,
			images:   map[string]string{"18": wlImage18},
			update:   new(testImageR),
			wantCode: codes.FailedPrecondition,
		},
		"white label in a cell without images is refused": {
			stored:   v1alpha1.SQLIdentityWhiteLabel,
			update:   new(testImageR),
			wantCode: codes.FailedPrecondition,
		},
		"white label keeps the image without an image update": {
			stored:    v1alpha1.SQLIdentityWhiteLabel,
			wantImage: testImageWL,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			existing := parentBranch(withParentSQLIdentity(tc.stored), func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Image = testImageWL
			})
			svc, k8sClient := setupTestClustersService(t,
				withSQLIdentityImages(tc.images),
				withExistingObjects(existing))

			_, err := svc.UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
				Id: existing.Name,
				UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
					ImageName:    tc.update,
					NumInstances: new(int32(3)),
				},
			})
			branch, getErr := getBranchFromK8s(ctx, k8sClient, existing.Name)
			require.NoError(t, getErr)
			if tc.wantCode != codes.OK {
				requireNeutralError(t, err, tc.wantCode)
				require.Equal(t, existing.Spec, branch.Spec)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantImage, branch.Spec.ClusterSpec.Image)
			require.Equal(t, int32(3), branch.Spec.ClusterSpec.Instances)

			id := shared.BranchSQLIdentity(branch)
			params := paramsMap(branch.Spec.ClusterSpec.Postgres.Parameters)
			require.Equal(t, privilegedExtensions(tc.wantImage, id), params[id.UtilsSettingPrefix()+"privileged_extensions"])
		})
	}
}
