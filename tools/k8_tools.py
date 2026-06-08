from kubernetes import client


_BAD_WAITING_REASONS = {"InvalidImageName", "ImagePullBackOff", "ErrImagePull"}


def _check_for_stuck_pods(core_v1: client.CoreV1Api, run_id: str, namespace: str) -> None:
    try:
        pods = core_v1.list_namespaced_pod(
            namespace=namespace, label_selector=f"job-name={run_id}"
        ).items
        for pod in pods:
            all_statuses = list(pod.status.init_container_statuses or []) + \
                           list(pod.status.container_statuses or [])
            for cs in all_statuses:
                if cs.state and cs.state.waiting and \
                        cs.state.waiting.reason in _BAD_WAITING_REASONS:
                    msg = cs.state.waiting.message or ""
                    raise RuntimeError(
                        f"Container '{cs.name}' cannot start: {cs.state.waiting.reason}"
                        + (f" — {msg}" if msg else "")
                    )
    except RuntimeError:
        raise
    except Exception:
        pass  # unable to check pod state, keep polling


def _collect_pod_logs(core_v1: client.CoreV1Api, run_id: str, namespace: str) -> str:
    """Collect logs from all containers of the Job's pod."""
    try:
        pods = core_v1.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"job-name={run_id}",
        ).items
        if not pods:
            return "(no pods found)"
        pod_name = pods[0].metadata.name
        lines = []
        for container in ["lakefs-pull", "model", "lakefs-push"]:
            try:
                log = core_v1.read_namespaced_pod_log(
                    name=pod_name, namespace=namespace,
                    container=container, tail_lines=50,
                )
                if isinstance(log, bytes):
                    log = log.decode("utf-8", errors="replace")
                lines.append(f"--- {container} ---\n{log}")
            except Exception as e:
                lines.append(f"--- {container} --- (could not retrieve: {e})")
        return "\n".join(lines)
    except Exception as e:
        return f"(could not collect pod logs: {e})"
