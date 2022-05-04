import looker_sdk
from looker_sdk.sdk.api40.models import WriteApiSession, WriteGitBranch, ProjectValidation
from git import Repo


def linter(looker_dir: str, project: str) -> ProjectValidation:
    sdk = looker_sdk.init40()
    repo = Repo.init(looker_dir)

    sdk.update_session(WriteApiSession(workspace_id="dev"))

    local_branch = str(repo.active_branch)
    remote_branches = [b.name for b in sdk.all_git_branches(project)]

    # git checkout
    if local_branch in remote_branches:
        sdk.update_git_branch(project, WriteGitBranch(name=local_branch))
    else:
        sdk.create_git_branch(project, WriteGitBranch(name=local_branch))

    # git pull from remote
    sdk.reset_project_to_remote(project)

    # lookml linting check
    result = sdk.validate_project(project)

    return result
