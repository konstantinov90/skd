import os.path

import git

from . import aio
from . import checks
from .db_client import db

col = db.cache
# db.commit.remove()

r = git.Repo('reg')
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
def init_cache():
    '''initialize cache'''
    aio.run(col.remove)
    aio.run(db.commit.remove)
    r.remotes.origin.pull('master')
    for system in r.tree().trees:
        for operation in system.trees:
            if operation.trees:
                raise Exception('supposed repo form condition is not met!')
            for file in operation.blobs:
                blob = checks.GitBlobWrapper(file)
                try:
                    check = blob.make_check()
                except checks.CheckExtError as e:
                    print(type(e), e)
                    continue
                col.insert(check.get_data())
    curr_commit['hash'] = r.commit().hexsha
    aio.run(db.commit.insert, curr_commit)

async def refresh_cache():
    '''pull and refresh cache'''
    await aio.async_run(r.remotes.origin.pull, 'master')
    if r.commit().hexsha == curr_commit['hash']:
        return
    diff = await aio.async_run(r.tree(curr_commit['hash']).diff, r.tree(r.commit().hexsha))
    for file in diff:
        blob_from = checks.GitBlobWrapper(file.a_blob) if file.a_blob else None
        blob_to = checks.GitBlobWrapper(file.b_blob) if file.b_blob else None
        check = blob_to.make_check() if blob_to else None
        if file.change_type == 'D':
            col.remove(blob_from.get_data())
        elif file.change_type == 'A':
            col.insert(check.get_data())
        elif file.change_type == 'M':
            col.update(blob_from.get_data(), check.get_data())
        else:
            raise ValueError('unexpected change_type for file {}'.format(blob_from.full_path))
        curr_commit["hash"] = r.commit().hexsha
        db.commit.update({}, curr_commit)


refresh_cache()
