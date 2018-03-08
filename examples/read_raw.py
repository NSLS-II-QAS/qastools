from databroker import Broker
import numpy as np
from qastools.handlers import (PizzaBoxEncHandlerTxt, PizzaBoxAnHandlerTxt,
                      PizzaBoxDIHandlerTxt)
import os
import matplotlib.pyplot as plt

db = Broker.named("qas")

db.reg.register_handler('PIZZABOX_AN_FILE_TXT',
                        PizzaBoxAnHandlerTxt, overwrite=True)
db.reg.register_handler('PIZZABOX_ENC_FILE_TXT',
                        PizzaBoxEncHandlerTxt, overwrite=True)
db.reg.register_handler('PIZZABOX_DI_FILE_TXT',
                        PizzaBoxDIHandlerTxt, overwrite=True)


uid = '09645d0a-cdb1-444e-96d1-3ec7e9f0795b'
hdr = db[uid]

dataset_name = 'pb1_enc1'
events = list(hdr.events(dataset_name))
datum = events[0]['data'][dataset_name]
resource = db.reg.resource_given_datum_id(datum)

resource_id = resource['uid']

# handler instance
handler = db.reg.get_spec_handler(resource_id)

# get the datum_kwargs, need a wrapper
datums = list(db.reg.datum_gen_given_resource(resource_id))
datum_kwargs = [datum['datum_kwargs'] for datum in datums]

file_list = handler.get_file_list(datum_kwargs)

gg=np.loadtxt(file_list[0])


plt.ion()
plt.figure()
plt.plot(gg[:,0])
plt.grid()
plt.figure()
plt.plot(gg[:,1])
plt.grid()
