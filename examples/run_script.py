from isstools.xasdata import xasdata
from databroker import Broker
from uuid import uuid4
from datetime import datetime
import os

# I don't like these handlers, we'll need to change them
# they return a table of list of objects which contain lists or something of
# the sort
from qastools.handlers import (PizzaBoxAnHandlerTxt, PizzaBoxEncHandlerTxt,
                               PizzaBoxDIHandlerTxt)
from qastools.interpolate import interpolate_and_save, bin_data

db_name = "qas"
db_analysis_name= "qas-analysis"

# db[-1].start['uid']
uid = '09645d0a-cdb1-444e-96d1-3ec7e9f0795b'

db.reg.register_handler('PIZZABOX_AN_FILE_TXT',
                        PizzaBoxAnHandlerTxt, overwrite=True)
db.reg.register_handler('PIZZABOX_ENC_FILE_TXT',
                        PizzaBoxEncHandlerTxt, overwrite=True)
db.reg.register_handler('PIZZABOX_DI_FILE_TXT',
                        PizzaBoxDIHandlerTxt, overwrite=True)


interpolate_and_save(db_name, db_analysis_name,
    uid, mono_name='mono1_enc', pulses_per_degree=None)

