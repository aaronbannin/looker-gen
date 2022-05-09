import looker_sdk
from looker_sdk.sdk.api40.models import WriteApiSession, WriteGitBranch
from git import Repo

from looker_gen.logging import log


def linter(looker_dir: str, project: str, test_content: bool) -> None:
    sdk = looker_sdk.init40()
    repo = Repo.init(looker_dir)

    sdk.update_session(WriteApiSession(workspace_id="dev"))

    local_branch = str(repo.active_branch)
    remote_branches = [b.name for b in sdk.all_git_branches(project)]
    log.debug(f'Using git branch {local_branch}')

    # git checkout
    if local_branch in remote_branches:
        sdk.update_git_branch(project, WriteGitBranch(name=local_branch))
    else:
        sdk.create_git_branch(project, WriteGitBranch(name=local_branch))

    # git pull from remote
    sdk.reset_project_to_remote(project)

    # lookml linting check
    validation = sdk.validate_project(project)

    if len(validation.errors) == 0:
        log.info('No linting errors!')
    else:
        log.error('Formatting errors found')
        for error in validation.errors:
            log.error(f'{error.severity} {error.file_path} {error.message}')

    if test_content:
        content = sdk.content_validation()
        content_errors = len(content.content_with_errors)
        log.error(f'{content_errors} content errors')
