import ast

from kubernetes import client


_BAD_WAITING_REASONS = {"InvalidImageName", "ImagePullBackOff", "ErrImagePull"}


def _decode_log(raw) -> str:
    """Decode a pod log that may be bytes, a decoded str, or a str(bytes) repr."""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    s = raw if isinstance(raw, str) else str(raw)
    # kubernetes client sometimes returns str(bytes_obj), e.g. "b'line1\nline2'"
    if len(s) > 1 and s[0] == "b" and s[1] in ("'", '"'):
        try:
            return ast.literal_eval(s).decode("utf-8", errors="replace")
        except Exception:
            pass
    return s


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
                raw = core_v1.read_namespaced_pod_log(
                    name=pod_name, namespace=namespace,
                    container=container, tail_lines=200,
                )
                log = _decode_log(raw).strip()
            except Exception as e:
                log = f"(could not retrieve: {e})"
            lines.append(f"{'=' * 40}\n  {container}\n{'=' * 40}\n{log}")
        return "\n\n".join(lines)
    except Exception as e:
        return f"(could not collect pod logs: {e})"
