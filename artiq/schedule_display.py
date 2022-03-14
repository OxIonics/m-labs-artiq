from artiq.tools import elide


def make_exp_source(expid, repo_msg, worker_managers=None):
    source_lines = []
    mgr_id = expid.get("worker_manager_id")
    if mgr_id is not None:
        mgr_desc = mgr_id
        if worker_managers is not None:
            try:
                mgr_desc = worker_managers[mgr_id]["description"]
            except KeyError:
                pass
        source_lines.append(f"mgr: {mgr_desc}")

    repo_rev = expid.get("repo_rev")
    if repo_rev is None:
        source_lines.append("Outside repo")
    elif repo_rev == "N/A":
        source_lines.append("Inside repo")
    else:
        source_lines.append(f"repo@{repo_rev}")
        if repo_msg:
            source_lines.append(elide(repo_msg, 40))
    return "\n".join(source_lines)
