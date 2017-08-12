import os.path

import git
import pymongo

from . import checks

cli = pymongo.MongoClient('vm-ts-blk-app2')
db = cli.skd_cache
col = db.cache
# db.commit.remove()

r = git.Repo('reg')
(curr_commit,) = db.commit.find()

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
    col.remove()
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
    curr_commit["hash"] = r.commit().hexsha
    db.commit.update({}, curr_commit, upsert=True)

def refresh_cache():
    '''pull and refresh cache'''
    r.remotes.origin.pull('master')
    if r.commit().hexsha == curr_commit['hash']:
        return
    diff = r.tree(curr_commit['hash']).diff(r.tree(r.commit().hexsha))
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
