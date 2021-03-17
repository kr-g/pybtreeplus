import json


class ReprDict(object):
    def __repr_dict__(self):
        raise NotImplementedError("function __repr_dict__ missing")

    def __repr__(self):
        return json.dumps(self.__repr_dict__(), indent=4)


class ReprDictVal(ReprDict):
    def __init__(self, val):
        self.val = val

    def __repr_dict__(self):
        return self.val


class LogTrace(ReprDict):
    def __init__(self):
        self.logtrace = {}

    def add(self, clsnam, objid, *infotxt, rel_key=None):
        if rel_key == None:
            rel_key = []
        if type(rel_key) != list:
            rel_key = [rel_key]
        rel_key.append(objid)
        for k in rel_key:
            c = self.logtrace.setdefault(str(clsnam), {})
            o = c.setdefault(str(k), [])
            for t in infotxt:
                o.append(str(t))
            o.append("-" * 7)

    def log(self, obj, *infotxt, keyfunc=None, key=None):
        objid = keyfunc(obj) if keyfunc else (getattr(obj, key) if key else None)
        self.add(obj.__class__.__name__, objid, *infotxt)

    def get(self, clsnam, objid=None):
        c = self.logtrace.get(str(clsnam), {})
        if objid == None:
            return c
        o = c.get(str(objid), [])
        return o

    def gets(self, clsnam, objid=None):
        val = self.get(clsnam, objid)
        return ReprDictVal(val)


class Dumy(object):
    def __init__(self, v):
        self.v = v

    def __repr__(self):
        return str(self.v)
