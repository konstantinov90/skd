import datetime
import os.path
import re

import git

import settings as S
from utils import aio
from utils import app_log
from utils.db_client import db
from . import checks
from . import kerberos_auth

LOG = app_log.get_logger(__name__)

col = db.cache
# db.commit.remove()

# r = git.Repo('reg')
repos_dir = 'reg'
curr_commit = {}

# supposed repo form
# system_code/
#             operation1_code/
#                            check1.py
#                            check2.sql
#                            check2.meta
#             operation2_code/
#                            check1.py
#                            check2.sql
#                            check2.meta
# system_code/
#             operation1_code/
#                            check1.py
#                            check2.sql
#                            check2.meta
#######################################################################
class Cache(object):
    def __init__(self):
        '''initialize cache'''
        self.repos = {}
        aio.run(col.remove)
        aio.run(db.commit.remove)
        if not os.path.isdir(repos_dir):
            os.mkdir(repos_dir)

        kerberos_auth.kinit()

        for repo_name, url in S.REPOS.items():
            repo_path = os.path.join(repos_dir, repo_name)
            try:
                repo = git.Repo(repo_path)
                repo.remotes.origin.pull('master')
            except git.exc.NoSuchPathError:
                repo = git.Repo.clone_from(url, repo_path)
            self.repos[repo_name] = repo
            for operation in repo.tree().trees:
                if operation.trees:
                    raise Exception('supposed repo form condition is not met!')
                for file in operation.blobs:
                    blob = checks.GitBlobWrapper(file)
                    try:
                        check = aio.run(blob.make_check)
                    except checks.CheckExtError as e:
                        LOG.warning('file {} ignored', blob)
                        continue
                    aio.run(col.insert, check.data)
            curr_commit[repo_name] = repo.commit().hexsha
        aio.run(db.commit.insert, curr_commit)

        self.refreshing = True
        self.future, self.waiting = None, None

    async def refresh(self):
        '''pull and refresh cache'''
        for repo_name, repo in self.repos.items():
            await aio.aio.wait_for(aio.async_run(repo.remotes.origin.pull, 'master'), 60)
            if repo.commit().hexsha == curr_commit[repo_name]:
                continue
            diff = await aio.async_run(repo.tree(curr_commit[repo_name]).diff, repo.tree(repo.commit().hexsha))
            for file in diff:
                blob_from = checks.GitBlobWrapper(file.a_blob) if file.a_blob else None
                blob_to = checks.GitBlobWrapper(file.b_blob) if file.b_blob else None
                try:
                    check = (await blob_to.make_check()) if blob_to else None
                except checks.CheckExtError:
                    LOG.warning('file {} ignored', blob_to)
                    file.change_type = 'D'

                if file.change_type == 'D':
                    await col.remove(blob_from.data)
                elif file.change_type == 'A':
                    await col.insert(check.data)
                elif re.match(r'^R\d{3}$|^M$', file.change_type):
                    await col.update(blob_from.data, check.data, upsert=True)
                else:
                    raise ValueError('unexpected change_type for file {}', blob_from.full_path)
                curr_commit[repo_name] = repo.commit().hexsha
        await db.commit.update({}, curr_commit)

    async def refresher(self):
        timeout_error_wait = 1
        while True:
            self.future = aio.aio.ensure_future(self.refresh())
            self.waiting = aio.aio.ensure_future(aio.aio.sleep(S.CACHE_REFRESH_SECONDS))
            try:
                await aio.aio.gather(self.waiting, self.future)
            except aio.aio.CancelledError:
                pass
            except aio.aio.TimeoutError:
                LOG.warning('GIT pull timeout, waiting {} before reconnect', timeout_error_wait)
                await aio.aio.sleep(timeout_error_wait)
                timeout_error_wait = min(2 * timeout_error_wait, 300)
                continue
            except Exception as exc:
                LOG.error('{}: {}', type(exc), exc)
            if not self.refreshing:
                await self.future
                break
            LOG.info('git tick')

    def stop(self):
        self.refreshing = False
        self.waiting.cancel()

_CACHE = Cache()

async def refresher():
    return await _CACHE.refresher()

def stop():
    return _CACHE.stop()
