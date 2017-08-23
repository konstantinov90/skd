import bson
import datetime
import json
from bson import ObjectId

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (ObjectId, datetime.datetime)):
            return str(o)
        elif isinstance(o, datetime.datetime):
            return
        return json.JSONEncoder.default(self, o)

def to_object_id(obj):
    if isinstance(obj, str):
        try:
            return bson.ObjectId(obj)
        except bson.errors.InvalidId:
            return obj
    elif isinstance(obj, dict):
        return {k: to_object_id(v) if "_id" in k or "$" in k else v
                for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_object_id(v) for v in obj]
    return obj

dumps = JSONEncoder(ensure_ascii=False).encode
