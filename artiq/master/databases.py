import asyncio
import copy
import tokenize

from artiq.tools import file_import
from sipyco.sync_struct import Notifier, process_mod, update_from_dict
from sipyco import pyon
from sipyco.asyncio_tools import TaskObject


def device_db_from_file(filename):
    mod = file_import(filename)

    # use __dict__ instead of direct attribute access
    # for backwards compatibility of the exception interface
    # (raise KeyError and not AttributeError if device_db is missing)
    return mod.__dict__["device_db"]


class DeviceDB:
    def __init__(self, backing_file):
        self.backing_file = backing_file
        self.data = Notifier(device_db_from_file(self.backing_file))

    def scan(self):
        update_from_dict(self.data, device_db_from_file(self.backing_file))

    def get_device_db(self):
        return self.data.raw_view

    def get(self, key, resolve_alias=False):
        desc = self.data.raw_view[key]
        if resolve_alias:
            while isinstance(desc, str):
                desc = self.data.raw_view[desc]
        return desc


def make_dataset(*, persist=False, value=None, hdf5_options=None):
    "PYON-serializable representation of a dataset in the DatasetDB"
    return {
        "persist": persist,
        "value": value,
        "hdf5_options": hdf5_options or {},
    }


class DatasetDB(TaskObject):
    def __init__(self, persist_file, autosave_period=30):
        self.persist_file = persist_file
        self.autosave_period = autosave_period

        try:
            file_data = pyon.load_file(self.persist_file)
        except FileNotFoundError:
            file_data = dict()

        self.data = Notifier(
            {
                k: make_dataset(
                    persist=True,
                    value=v["value"],
                    hdf5_options=v["hdf5_options"]
                )
                for k, v in file_data.items()
            }
        )

    def save(self):
        data = {
            k: d
            for k, d in self.data.raw_view.items()
            if d["persist"]
        }
        pyon.store_file(self.persist_file, data)

    async def _do(self):
        try:
            while True:
                await asyncio.sleep(self.autosave_period)
                self.save()
        finally:
            self.save()

    def get(self, key):
        return self.data.raw_view[key]

    def update(self, mod):
        process_mod(self.data, mod)

    # convenience functions (update() can be used instead)
    def set(self, key, value, persist=None, hdf5_options=None):
        if persist is None:
            if key in self.data.raw_view:
                persist = self.data.raw_view[key]["persist"]
            else:
                persist = False
        self.data[key] = make_dataset(
            persist=persist,
            value=value,
            hdf5_options=hdf5_options,
        )

    def delete(self, key):
        del self.data[key]
<<<<<<< HEAD
    #


def _rid_namespace_name(rid):
    return "datasets_rid_{}".format(rid)


class DatasetNamespaces:
    """Manages source namespaces for datasets.
    """

    def __init__(self, dataset_db, gc_timeout_seconds = 10 * 60):
        #: Duration namespaces are kept alive (for clients to access) after they
        #: have been finished, in seconds.
        self.gc_timeout_seconds = gc_timeout_seconds

        self._dataset_db = dataset_db
        self._publisher = None
        self._rid_notifiers = dict()
        self._gc_tasks = dict()

    async def stop(self):
        tasks = self._gc_tasks.values()
        if not tasks:
            return
        for t in tasks:
            t.cancel()
        await asyncio.wait(tasks)

    def set_publisher(self, publisher):
        assert self._publisher is None
        self._publisher = publisher
        for rid in self._rid_notifiers.keys():
            self._publish_rid(rid)

    def init_rid(self, rid):
        notifier = Notifier(dict())
        self._rid_notifiers[rid] = notifier
        self._publish_rid(rid)

    def update_rid_namespace(self, rid, mod):
        process_mod(self._rid_notifiers[rid], mod)
        # Forward to global namespace as well.
        self._dataset_db.update(copy.deepcopy(mod))

    def finish_rid(self, rid):
        if rid not in self._rid_notifiers:
            return

        async def gc():
            await asyncio.sleep(self.gc_timeout_seconds)
            self._publisher.remove_notifier(_rid_namespace_name(rid))
            del self._rid_notifiers[rid]
            del self._gc_tasks[rid]
        self._gc_tasks[rid] = asyncio.ensure_future(gc())

    def _publish_rid(self, rid):
        if self._publisher:
            self._publisher.add_notifier(_rid_namespace_name(rid), self._rid_notifiers[rid])
=======
>>>>>>> 5e1847
