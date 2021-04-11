import json

if __name__ == '__main__':

    file_names = ['ADANIPORTS_3861249.json', 'ASIANPAINT_60417.json', 'BAJAJ-AUTO_4267265.json', 'BAJAJFINSV_4268801.json',
                  'BHARTIARTL_2714625.json', 'BPCL_134657.json', 'BRITANNIA_140033.json', 'CIPLA_177665.json',
                  'DIVISLAB_2800641.json', 'DRREDDY_225537.json', 'EICHERMOT_232961.json', 'GRASIM_315393.json',
                  'HCLTECH_1850625.json', 'HDFC_340481.json', 'HDFCBANK_341249.json', 'HEROMOTOCO_345089.json',
                  'HINDPETRO_359937.json', 'HINDUNILVR_356865.json', 'INDUSINDBK_1346049.json', 'INFY_408065.json',
                  'IOC_415745.json', 'ITC_424961.json', 'JSWSTEEL_3001089.json', 'KOTAKBANK_492033.json', 'LT_2939649.json',
                  'MARUTI_2815745.json', 'M&M_519937.json', 'NESTLEIND_4598529.json', 'NTPC_2977281.json',
                  'ONGC_633601.json', 'POWERGRID_3834113.json', 'RELIANCE_738561.json', 'SAIL_758529.json',
                  'SBILIFE_5582849.json', 'SBIN_779521.json', 'SHREECEM_794369.json', 'SUNPHARMA_857857.json',
                  'TATACONSUM_878593.json', 'TATAMOTORS_884737.json', 'TECHM_3465729.json', 'TITAN_897537.json',
                  'ULTRACEMCO_2952193.json', 'UPL_2889473.json', 'WIPRO_969473.json']

    folders = ['2015_16', '2016_17', '2017_18', '2018_19', '2019_20', '2020_21', '2021']
    json_base_path = '/data/kite_websocket_data/historical/'
    csv_base_path = '/data/kite_websocket_data/historical/csv_files/'

    for folder in folders:
        for file_name in file_names:
            json_file_path = json_base_path + folder + "/" + file_name
            output_file_path = csv_base_path + folder + "/" + file_name.replace(".json", ".csv")
            with open(json_file_path) as handle:
                arr = json.load(handle)

            header = "date,open,high,low,close,volume\n"
            rows = [",".join([str(e) for e in elem]) + "\n" for elem in arr]
            f = open(output_file_path, 'w')
            f.write(header)
            for row in rows:
                f.write(row)
            f.close()

