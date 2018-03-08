import uuid
from databroker import Broker
import time
import numpy as np
import os
import os.path as op
import jsonschema
from event_model import DocumentNames, schemas


from pathlib import Path

# setup of objects to write to databroker

from databroker.assets.handlers_base import HandlerBase
class DataFrameWriter(HandlerBase):
    """
    Class to handle writing a numpy array out to disk and registering
    that write with Registry.

    This class is only good for one call to add_data.

    Parameters
    ----------
    fpath : str
        Path (including filename) of where to save the file

    resource_kwargs : dict, optional
        Saved in the resource_kwargs field of the fileBase document.
    """
    SPEC_NAME = 'pandas'
    def __init__(self, fpath, reg, root=None, prefix=""):
        if op.exists(fpath):
            raise IOError(f"the requested file {fpath} already exist")
        self._fpath = fpath
        self._root = root
        # no resource for now
        self._resource_kwargs = {}
        self._prefix = prefix

        self._writable = True
        self._reg = reg
        # check if file path exists, if not, create
        full_filepath = Path(root) / Path(fpath)
        os.makedirs(full_filepath, exist_ok=True)

    def add_data(self, data, uid=None, datum_kwargs={}):
        """
        Parameters
        ----------
        data : ndarray
            The data to save

        uid : str, optional
            The uid to be used for this entry,
            if not given use uuid1 to generate one

            .. warning ::

               This may only be taken as a suggestion.

        datum_kwargs : None, optional
            should be blank, nothing accepted yet

        Returns
        -------
        uid : str
            The uid used to register this data with an asset registry, can
            be used to retrieve it
        """
        if not self._writable:
            raise RuntimeError("This writer can only write one data entry "
                               "and has already been used")

        if resource_kwargs:
            raise ValueError("This writer does not support resource_kwargs")

        if op.exists(self._fpath):
            raise IOError("the requested file {fpath} "
                          "already exist".format(fpath=self._fpath))

        if uid is None:
            uid = str(uuid.uuid4())

        filename = Path(self._root) / Path(self._fpath) / Path(self._prefix + str(uuid.uuid4())+".txt")
        data.to_csv(filename, sep="\t")
        self._writable = False
        # insert_resource(spec name, resource path, additional kwargs)
        fb = self._reg.insert_resource(self.SPEC_NAME,
                                     filename,
                                     f._resource_kwargs, root=self._root)

        # give the resource, uid and kwargs
        evl = self._reg.insert_datum(fb, uid, datum_kwargs)

        return evl['datum_id']

# writing a file handler is here
from databroker.assets.handlers import NpyHandler

# and define the writer to write to file and return some descriptor
# an example of writing to databroker 
# db is the databroker object
# db.mds is the metadatastore
# TODO : provenance?
# parent_uid : ...
# parent_database : qas
def ingest_interp_df(df, filepath, db_name, md={}, root=None,
                     source="QAS-Analysis"):
    ''' Save results to a databroker instance.

        Parameters
        ----------

        df : the dataframe
        filepath: the full filename of file you want to write
            needs to be an absolute path
        md : the metadata
        db_name : the name of the Broker instance
            or a Broker instance

        It is assumed everything here can be written with the same writer.
    '''
    if isinstance(db, str):
        db = Broker.named(db_name)
    else:
        db = db_name
    # need the metadatastore of databroker
    #mds = db.mds  # metadatastore

    # This part creates all the documents
    # Store in databroker, make the documents
    start_doc = dict()
    # start_doc.update(attributes)
    # update the start doc with the metadata
    # TODO : move to create_start... update_desc etc...
    start_doc.update(**md)
    start_doc['time'] = time.time()
    start_doc['uid'] = str(uuid.uuid4())
    start_doc['plan_name'] = 'analysis'
    start_doc['save_timestamp'] = time.time()
    # need stream names
    start_doc['name'] = 'primary'

    # just make one descriptor and event document for now
    # initialize both event and descriptor
    descriptor_doc = dict()
    event_doc = dict()
    event_doc['data'] = dict()
    event_doc['timestamps'] = dict()
    descriptor_doc['data_keys'] = dict()
    descriptor_doc['time'] = time.time()
    descriptor_doc['uid'] = str(uuid.uuid4())
    descriptor_doc['run_start'] = start_doc['uid']
    descriptor_doc['name'] = 'primary'
    #descriptor_doc['object_keys'] = dict()

    event_doc['time'] = time.time()
    event_doc['uid'] = str(uuid.uuid4())
    event_doc['descriptor'] = descriptor_doc['uid']
    event_doc['seq_num'] = 1
    event_doc['filled'] = dict()

    # then parse remaining data
    descriptor_doc['data_keys']['interp_df'] = make_descriptor(df, source=source)
    # save to filestore
    # instantiate class with db.reg
    time_now = time.localtime()
    subpath = "/{:04}/{:02}/{:02}".format(time_now.tm_year,
                                          time_now.tm_mon,
                                          time_now.tm_mday)
    writer = DataFrameWriter(filepath+subpath, db.reg, root=root)
    new_id = writer.add_data(df)
    # at key give the identifier not the value
    event_doc['data'][key] = new_id
    event_doc['filled'][key] = False
    descriptor_doc['data_keys'][key].update(external="FILESTORE:")
    event_doc['timestamps'][key] = time.time()


    stop_doc = dict()
    stop_doc['time'] = time.time()
    stop_doc['uid'] = str(uuid.uuid4())
    stop_doc['run_start'] = start_doc['uid']
    stop_doc['exit_status'] = 'success'
    stop_doc['name'] = 'primary'

    # write the database results here to mongodb
    jsonschema.validate(start_doc, schemas[DocumentNames.start])
    jsonschema.validate(descriptor_doc, schemas[DocumentNames.descriptor])
    jsonschema.validate(event_doc, schemas[DocumentNames.event])
    jsonschema.validate(stop_doc, schemas[DocumentNames.stop])
    db.insert('start', start_doc)
    db.insert('descriptor', descriptor_doc)
    db.insert('event', event_doc)
    db.insert('stop', stop_doc)

    docs = list()
    docs.append(('start', start_doc))
    docs.append(('descriptor', descriptor_doc))
    docs.append(('event', event_doc))
    docs.append(('stop', stop_doc))
    return docs


# quick dummy script to make a descriptor
def make_descriptor(val, source=None):
    ''' make a descriptor from value through guessing.'''
    if source is None:
        raise ValueError("Error source must be defined")
    shape = ()
    if np.isscalar(val):
        dtype = 'number'
    elif isinstance(val, np.ndarray):
        dtype = 'array'
        shape = val.shape
    elif isinstance(val, list):
        dtype = 'list'
        shape = (len(val),)
    elif isinstance(val, dict):
        dtype = 'dict'
    else:
        dtype = 'unknown'

    return dict(dtype=dtype, shape=shape, source=source)


# md is a dict of metadata
md = dict(sample_name = "foo", user="Bruno")
# data is a dict of some data (could be something else)
# NOTE : With the way data is saved, you need to make the list a np.array()
data = dict(file1=np.array([2,3,5]), file2=np.array([3,4,5]))


# probably a good idea to define some writers for each data element
external_writers = {
    'data1' : NpyWriter,
    'data2' : NpyWriter,
}


db_analysis = Broker.named("iss-analysis")
rootpath = '/GPFS/xf08id'
filepath = 'Sandbox/'
store_results_databroker(md, data, db_analysis, filepath, root=rootpath)


# retrieval
# first register resource
from databroker.assets.handlers import NpyHandler
db_analysis.reg.register_handler('NPY', NpyHandler)

# now retrieve
hdr = db_analysis[-1]

# get table format
table = hdr.table()

# get table format with events filled
table = hdr.table(fill=True)

data1 = table.file1
# make sure they're equal (access pandas table by sequence number, here "1"
#   data1[1])
assert np.allclose(data['file1'], data1[1], atol=0)

