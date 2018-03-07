from isstools.xasdata import xasdata
from databroker import Broker
from uuid import uuid4
from datetime import datetime
import os

db = Broker.named("qas")
db_analysis = Broker.named("qas-analysis")

# db[-1].start['uid']
uid = '09645d0a-cdb1-444e-96d1-3ec7e9f0795b'

# TODO : make package rather than script
from handlers import (PizzaBoxAnHandlerTxt, PizzaBoxEncHandlerTxt,
                      PizzaBoxDIHandlerTxt)

db.reg.register_handler('PIZZABOX_AN_FILE_TXT',
                        PizzaBoxAnHandlerTxt, overwrite=True)
db.reg.register_handler('PIZZABOX_ENC_FILE_TXT',
                        PizzaBoxEncHandlerTxt, overwrite=True)
db.reg.register_handler('PIZZABOX_DI_FILE_TXT',
                        PizzaBoxDIHandlerTxt, overwrite=True)

def interpolate_and_save(db, db_analysis,
                         uid, mono_name='mono1_enc'):
    ''' Interpolate measured data and save to an analysis store. 

        Parameters
        ----------
        # TODO : change to config
        db_config: dict
            the config for the database
        db_analysis_config : dict
            the config for the analysis database
        uid : str
            The uid of the data set
        mono_name : str
            the monochromator encoder name. Defaults to 'mono1_enc'
    '''
    # re-instantiate from config (so this can be done remotely)
    #db = Broker.from_config(db_config)
    #db_analysis = Broker.from_config(db_analysis_config)

    # the pulses per degree, hard coded for now
    # TODO : Make a signal to pb1.enc1
    # and have it passed at configuration_attrs 
    # (which results in data in descriptor)
    ppd=23600*400/360

    # the important part of Bruno's code that does the interpolation
    gen_parser= xasdata.XASdataGeneric(ppd, db=db, mono_name='mono1_enc')
    gen_parser.load(uid)
    # data saves in gen_parser.interp_df
    gen_parser.interpolate() 

    # useful command for debugging, looking at energy
    # this is automatically run by gen_parser
    #energy = encoder2energy(res[:,3], ppd)

    # TODO : understand this step better: get the edge and plot
    # commented out for now
    e0 = 9057

    bin_df = gen_parser.bin(e0, e0 - 30, e0 + 30, 4, 0.2, 0.04)

    PREFIX = "/nsls2/xf07bm/data/interpolated_data"
    write_path_template = PREFIX + '/%Y/%m/%d'
    DIRECTORY = datetime.now().strftime(write_path_template)

    filename = 'xas_' + str(uuid4())[:6]
    os.makedirs(DIRECTORY)
    filepath = DIRECTORY

    # file is exported
    filepath = gen_parser.export_trace(filename, filepath)

    return bin_df

bin_df = interpolate_and_save(db, db_analysis,
                         uid, mono_name='mono1_enc')

