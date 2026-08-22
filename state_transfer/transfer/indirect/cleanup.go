package indirect

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CleanupCloudData creates a pod that deletes transferred data from cloud storage.
// Uses the direct S3 path (not the crypt overlay) so it works regardless of
// whether encryption was enabled — encrypted file names are still under the
// same S3 prefix.
func (t *IndirectTransfer) CleanupCloudData(ctx context.Context, c client.Client, namespace, pvcName, sourceNamespace, sourcePVCName string) (*corev1.Pod, error) {
	if err := t.options.Validate(); err != nil {
		return nil, fmt.Errorf("invalid options for cloud cleanup: %w", err)
	}
	remotePath := fmt.Sprintf("%s/%s/%s", t.options.CloudStorage, sourceNamespace, sourcePVCName)
	// Use "rclone sync" from an empty directory to the remote path.
	// This deletes all files on the remote since the source is empty.
	// We use sync instead of purge/delete because our minimal rclone
	// binary only includes the sync command.
	emptyDir := "/tmp/empty"
	command := []string{
		"sh", "-c",
		fmt.Sprintf("mkdir -p %s && exec rclone sync %s \"$1\" --config %s/rclone.conf -v",
			emptyDir, emptyDir, configMountPath),
		"--",
		remotePath,
	}

	podLabels := copyLabels(t.options.Labels)
	podLabels["app.konveyor.io/created-for-pvc"] = pvcName

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      truncatePodName(fmt.Sprintf("rclone-cleanup-%s", pvcName)),
			Namespace: namespace,
			Labels:    podLabels,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:    "rclone",
					Image:   t.options.Image,
					Command: command,
					VolumeMounts: []corev1.VolumeMount{
						{Name: configVolumeName, MountPath: configMountPath, ReadOnly: true},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: configVolumeName,
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: t.options.ConfigSecret,
						},
					},
				},
			},
		},
	}

	if err := c.Create(ctx, pod); err != nil {
		return nil, fmt.Errorf("failed to create cloud cleanup pod: %w", err)
	}
	return pod, nil
}

// Cleanup deletes transfer pods and secrets scoped by PVC label.
func (t *IndirectTransfer) Cleanup(ctx context.Context, c client.Client, namespace, pvcName string) error {
	if len(t.options.Labels) == 0 {
		return fmt.Errorf("refusing to cleanup with empty labels: would match all resources in namespace %s", namespace)
	}
	cleanupLabels := copyLabels(t.options.Labels)
	cleanupLabels["app.konveyor.io/created-for-pvc"] = pvcName

	// Delete pods
	podList := &corev1.PodList{}
	if err := c.List(ctx, podList,
		client.InNamespace(namespace),
		client.MatchingLabels(cleanupLabels)); err != nil {
		return fmt.Errorf("failed to list indirect transfer pods: %w", err)
	}
	for i := range podList.Items {
		if err := c.Delete(ctx, &podList.Items[i]); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("failed to delete pod %s: %w", podList.Items[i].Name, err)
		}
	}

	// Delete secrets
	secretList := &corev1.SecretList{}
	if err := c.List(ctx, secretList,
		client.InNamespace(namespace),
		client.MatchingLabels(cleanupLabels)); err != nil {
		return fmt.Errorf("failed to list indirect transfer secrets: %w", err)
	}
	for i := range secretList.Items {
		if err := c.Delete(ctx, &secretList.Items[i]); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("failed to delete secret %s: %w", secretList.Items[i].Name, err)
		}
	}

	return nil
}
