from isstools.xasdata import xasdata
from databroker import Broker
from uuid import uuid4
from datetime import datetime
import os

def interpolate_and_save(db_name, db_analysis_name,
                         uid, mono_name='mono1_enc',
                         pulses_per_degree=None, e0=None):
    ''' Interpolate measured data and save to an analysis store. 

        Parameters
        ----------
        # TODO : change to config (don't rely on Broker.named which explores
            local directory)
        db_config: str
            the name the database (in /etc/databroker/name.yml)
        db_analysis_config : str
            the name of the analysis database (in /etc/databroker/name.yml)
        uid : str
            The uid of the data set
        mono_name : str
            the monochromator encoder name. Defaults to 'mono1_enc'
        pulses_per_degree : float
            pulses per degree of the encoder from the monochromator
            defaults to the current setup at QAS
        e0 : 
            edge energy. If set, compute binned data

        Returns
        -------
            interp_df : the interpolated data
            bin_df : the binned data if e0 is set
    '''
    # the pulses per degree, hard coded for now
    # TODO : Make a signal to pb1.enc1
    # and have it passed at configuration_attrs 
    # (which results in data in descriptor)
    if pulses_per_degree is None:
        ppd=23600*400/360
    else:
        ppd = pulses_per_degree

    db = Broker.named(db_name)
    hdr = db[uid]

    db_analysis = Broker.named(db_analysis_name)

    # the important part of Bruno's code that does the interpolation
    gen_parser= xasdata.XASdataGeneric(ppd, db=db, mono_name='mono1_enc')
    gen_parser.load(uid)
    # data saves in gen_parser.interp_df
    gen_parser.interpolate() 

    # useful command for debugging, looking at energy
    # this is automatically run by gen_parser
    #energy = encoder2energy(res[:,3], ppd)

    PREFIX = "/nsls2/xf07bm/data/interpolated_data"
    write_path_template = PREFIX + '/%Y/%m/%d/'
    DIRECTORY = datetime.now().strftime(write_path_template)
    scan_id = hdr.start['scan_id']

    md = hdr.start
    filename = 'xas_' + md.get("name", str(uuid4())[:6]) + "_" + str(scan_id)
    os.makedirs(DIRECTORY,exist_ok=True)
    filepath = DIRECTORY

    # file is exported
    fileout = gen_parser.export_trace(filename, filepath)


    if e0 is not None:
        bin_df, bin_df_filename = bin_data(gen_parser, fileout, e0, scan_id=scan_id)
    else:
        bin_df, bin_df_filename = None, None

    result = dict(bin_df=bin_df,
                  bin_df_filename=bin_df_filename,
                  interp_df=gen_parser.interp_df,
                  interp_df_filename=fileout,
                  scan_id=scan_id
                  )

    return result


def bin_data(gen_parser, binned_file, e0, scan_id=""):
    ''' Bin the data according to iss binning algorithm.

        Parameters
        ----------
            e0 : the energy of the edge
            scan_id : optional scan_id for the file writing

        Returns
        -------
            bin_df : a dataframe of the binned data
    '''
    # TODO : understand this step better: get the edge and plot
    # commented out for now
    bin_df = gen_parser.bin(e0, e0 - 30, e0 + 30, 4, 0.2, 0.04)

    #PREFIX = "/nsls2/xf07bm/data/interpolated_data"
    #write_path_template = PREFIX + '/%Y/%m/%d/'
    #DIRECTORY = datetime.now().strftime(write_path_template)

    #filename = 'xas_' + str(uuid4())[:6]
    #filename = 'xas_' + str(uuid4())[:6] + str(scan_id)
    #os.makedirs(DIRECTORY,exist_ok=True)
    #filepath = DIRECTORY

    # file is exported
    fileout = gen_parser.data_manager.export_dat(binned_file, e0)
    

    return bin_df, fileout
