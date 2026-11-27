"""Connections to the source systems (MongoDB, SQL) with a local fallback."""
import pickle

from bdi.config import DATA, MONGO_DB, MONGO_URI

_MOCK_FILE = DATA / "mongomock_specs.pkl"
_mock_client = None


def mongo_collection():
    """Real MongoDB when MONGO_URI is set (GitHub Actions service); otherwise an
    in-memory mongomock database persisted to disk between pipeline steps."""
    global _mock_client
    if MONGO_URI:
        from pymongo import MongoClient
        return MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)[MONGO_DB]["specs"]
    import mongomock
    if _mock_client is None:
        _mock_client = mongomock.MongoClient()
        if _MOCK_FILE.exists():
            _mock_client[MONGO_DB]["specs"].insert_many(pickle.loads(_MOCK_FILE.read_bytes()))
    return _MockCollection(_mock_client[MONGO_DB]["specs"])


class _MockCollection:
    """Wraps a mongomock collection and saves inserted documents so later steps can read them."""
    def __init__(self, coll):
        self._c = coll

    def __getattr__(self, name):
        return getattr(self._c, name)

    def insert_many(self, docs):
        result = self._c.insert_many(docs)
        _MOCK_FILE.write_bytes(pickle.dumps(list(self._c.find({}))))
        return result

    def delete_many(self, flt):
        if _MOCK_FILE.exists():
            _MOCK_FILE.unlink()
        return self._c.delete_many(flt)
