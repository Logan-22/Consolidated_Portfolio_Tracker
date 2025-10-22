from flask import Blueprint, jsonify, request
from os import path, remove
from datetime import datetime

# Folders

from utils.folder_utils.paths import upload_folder_path
from werkzeug.utils import secure_filename
from PyPDF2 import PdfReader

file_process_bp = Blueprint('file_process', __name__)

@file_process_bp.route('/stock_pdf/', methods = ['POST'])
def stock_order_entry_from_pdf():
    try:
        # Individual Trade Info
        trade_info = {}
        trade_list = []
        fees_info = {}

        # Remarks line number - The Next lines are usually trades info
        remarks_line_number = 0

        # Trade Agnostic Info
        trade_entry_date = ""
        net_obligation = ""
        brokerage = ""
        exc_trans_charges= ""
        igst = ""
        sec_trans_tax = ""
        sebi_turn_fees = ""

        # Derived Info
        derived_trade_info = {}


        if 'stock_pdf_file' not in request.files:
            return jsonify({'message': 'File was not Uploaded Successfully', 'status': 'Failed'})
        stock_pdf_file = request.files['stock_pdf_file']
        if stock_pdf_file.filename == "":
            return jsonify({'message': 'No File was Selected', 'status': 'Failed'})
        if stock_pdf_file and stock_pdf_file.filename.lower().endswith('.pdf'):
            stock_pdf_file_name = stock_pdf_file.filename.lower().replace(" ", "_")
            safe_stock_pdf_file_name = secure_filename(stock_pdf_file_name)

            stock_pdf_file_name_path = path.join(upload_folder_path, safe_stock_pdf_file_name)
            
            stock_pdf_file.save(stock_pdf_file_name_path)
        else:
            return jsonify({'message': 'Only PDF File Format is Accepted', 'status': 'Failed'})

        # Parse the uploaded PDF file from Upload Folder
        file_password = request.form.get('file_password')
        pdf_reader = PdfReader(stock_pdf_file_name_path)

        if pdf_reader.is_encrypted:
            pdf_reader.decrypt(file_password)

        text = ""

        for page in pdf_reader.pages: # Cleanse and Add to Variable 'Text'
            text += page.extract_text().replace("\ufb03","").replace("\ufb00","").replace("\u2074","").replace("\ufb01","").replace("\u20b9","").replace("�","").replace("^M","").replace("\r","").replace("�","")

        stock_text_file_name_path = stock_pdf_file_name_path.replace(".pdf", ".txt")

        with open(stock_text_file_name_path, "w+", encoding="utf-8") as text_file:
            text_file.write(text)

        with open(stock_text_file_name_path, "r") as read_text_file:
            for line_number, line in enumerate(read_text_file, start = 1):
                ## Trade Agnostic
                if "T rade Date" in line:  ### Check with time
                    semi_colon_index = line.rfind(":")
                    trade_entry_date = datetime.strptime(line[semi_colon_index + 1:].strip(), '%d/%m/%Y')
                    trade_entry_date = trade_entry_date.strftime('%Y-%m-%d')

                if "Pay" in line:  ### Check with time
                    space_index = line.rfind(" ")
                    net_obligation = line[space_index:]
                    net_obligation = net_obligation.strip(" ")
                    if "(" in net_obligation or ")" in net_obligation:
                        net_obligation = float(net_obligation.strip("(").replace(")","")) * -1
                    fees_info['net_obligation'] = net_obligation
                    
                if "T axable" in line:  ### Check with time
                    space_index = line.rfind(" ")
                    brokerage = line[space_index:]
                    brokerage = float(brokerage.strip(" ").strip("(").replace(")",""))
                    fees_info['brokerage'] = brokerage

                if "Exchange transaction charges" in line:  ### Check with time
                    space_index = line.rfind(" ")
                    exc_trans_charges = line[space_index:]
                    exc_trans_charges = float(exc_trans_charges.strip(" ").strip("(").replace(")",""))
                    fees_info['exc_trans_charges'] = exc_trans_charges

                if line.startswith("IGST"):  ### Check with time
                    space_index = line.rfind(" ")
                    igst = line[space_index:]
                    igst = float(igst.strip(" ").strip("(").replace(")",""))
                    fees_info['igst'] = igst

                if "transaction tax" in line:  ### Check with time
                    space_index = line.rfind(" ")
                    sec_trans_tax = line[space_index:]
                    if sec_trans_tax.strip(" ").strip("\n") == "tax":
                        sec_trans_tax = 0
                    else:
                        sec_trans_tax = float(sec_trans_tax.strip(" ").strip("(").replace(")",""))
                    fees_info['sec_trans_tax'] = sec_trans_tax

                if "SEBI turno" in line:  ### Check with time
                    space_index = line.rfind(" ")
                    sebi_turn_fees = line[space_index:]
                    if sebi_turn_fees.strip(" ").strip("\n") == "fe es":
                        sebi_turn_fees = 0
                    else:
                        sebi_turn_fees = float(sebi_turn_fees.strip(" ").strip("(").replace(")",""))
                    fees_info['sebi_turn_fees'] = sebi_turn_fees
                
                if "Remarks" in line: ### Check with Time
                    remarks_line_number = line_number

        with open(stock_text_file_name_path, "r") as read_text_file_for_trades:
            for line_number, line in enumerate(read_text_file_for_trades, start = 1):
                # Specific to each Trade
                if line_number > remarks_line_number: ### Check with Time
                    line_split                                          = line.split(" ")
                    order_number                                        = line_split[0]
                    trade_info[order_number]                            = {}
                    individual_trade_info                               = trade_info[order_number]
                    individual_trade_info['order_number']               = order_number                                       # 1000000005038948
                    individual_trade_info['order_time']                 = line_split[1]                                      # 09:28:11
                    individual_trade_info['trade_number']               = line_split[2]                                      # 990607
                    individual_trade_info['trade_time']                 = line_split[3]                                      # 09:28:11

                    remaining_line_split                                = line_split[4:]                                     # AD ANIEN T -EQ/INE423A01024 S NSE 1 2202.20 2202.2 2202.20
                    remaining_line_join                                 = ' '.join(remaining_line_split)                     # AD ANIEN T -EQ/INE423A01024 S NSE 1 2202.20 2202.2 2202.20
                    hyphen_index                                        = remaining_line_join.find("-")                      # 11
                    individual_trade_info['stock_symbol']               = remaining_line_join[:hyphen_index].replace(" ","") # ADANIENT

                    stock_type_isin_exchange_metrics                    = remaining_line_join[hyphen_index+1:]               # EQ/INE423A01024 S NSE 1 2202.20 2202.2 2202.20 # +1 to ignore Hyphen
                    stock_type_isin_split                               = stock_type_isin_exchange_metrics.split("/")        # EQ | INE423A01024 S NSE 1 2202.20 2202.2 2202.20
                    individual_trade_info['asset_type']                 = stock_type_isin_split[0]                           # EQ
                    stock_isin_exchange_metrics                         = stock_type_isin_split[1]                           # INE0LX G01040 B NSE 100 1.4889 49.63 (4963.00)
                    if " B " in stock_isin_exchange_metrics:
                        buy_or_sell_index                               = stock_isin_exchange_metrics.find(" B ")            # 13
                    if " S " in stock_isin_exchange_metrics:
                        buy_or_sell_index                               = stock_isin_exchange_metrics.find(" S ")            # 13
                    stock_isin_string                                   = stock_isin_exchange_metrics[:buy_or_sell_index]    # INE0LX G01040
                    individual_trade_info['stock_isin']                 = stock_isin_string.replace(" ","")                  # INE0LXG01040
                    stock_exchange_metrics                              = stock_isin_exchange_metrics[buy_or_sell_index:]    #  B NSE 100 1.4889 49.63 (4963.00)
                    stock_exchange_metrics_split                        = stock_exchange_metrics.split(" ")                  # B NSE 100 1.4889 49.63 (4963.00)
                    individual_trade_info['buy_or_sell']                = stock_exchange_metrics_split[1]                    # S
                    individual_trade_info['stock_exchange']             = stock_exchange_metrics_split[2]                    # NSE
                    individual_trade_info['stock_quantity']             = int(stock_exchange_metrics_split[3])               # 1

                    # Due to Contract Note Format Changes as of 01-Apr-2025
                    if trade_entry_date < '2025-04-01':
                        individual_trade_info['gross_trade_price_per_unit'] = float(stock_exchange_metrics_split[4])         # 2202.20
                        individual_trade_info['brokerage_per_trade']        = "Not Present"                                  # Not Present
                    elif trade_entry_date >= '2025-04-01':
                        individual_trade_info['brokerage_per_trade']        = float(stock_exchange_metrics_split[4])         # 1.4889
                        individual_trade_info['gross_trade_price_per_unit'] = "Not Present"                                  # Not Present
                    individual_trade_info['net_trade_price_per_unit']       = float(stock_exchange_metrics_split[5])         # 2202.2
                    net_total_before_levies                                 = stock_exchange_metrics_split[6]                # 2202.20

                    if "(" in net_total_before_levies:                                                                       # (1249.40)
                        net_total_before_levies = net_total_before_levies.replace(" ","").replace("(","").replace(")","")
                        
                    individual_trade_info['net_total_before_levies']    = float(net_total_before_levies)

                    trade_list.append(individual_trade_info)
        # Derived Trade Info
        for info in trade_list:
            derived_trade_info[info['stock_symbol']] = {}
        # Mark trades with trade set
        for stock_name in derived_trade_info:
            derived_trade_info[stock_name]['trade_entry_time'] = '15:30:00'
            derived_trade_info[stock_name]['trade_exit_time']  = '09:15:00'
            derived_trade_info[stock_name]['final_stock_quantity']   = 0
            trade_set = 1
            for info in trade_list:
                if info['stock_symbol'] == stock_name:
                    if datetime.strptime(info['trade_time'],'%H:%M:%S') < datetime.strptime(derived_trade_info[stock_name]['trade_entry_time'], '%H:%M:%S'):
                        derived_trade_info[stock_name]['trade_entry_time'] = info['trade_time']
                    if datetime.strptime(info['trade_time'],'%H:%M:%S') > datetime.strptime(derived_trade_info[stock_name]['trade_exit_time'], '%H:%M:%S'):
                        derived_trade_info[stock_name]['trade_exit_time'] = info['trade_time']
                    if info['buy_or_sell'] == 'B':
                        derived_trade_info[stock_name]['final_stock_quantity'] += info['stock_quantity']
                        info['trade_set'] = trade_set
                        if derived_trade_info[stock_name]['final_stock_quantity'] == 0:
                            trade_set += 1
                    if info['buy_or_sell'] == 'S':
                        derived_trade_info[stock_name]['final_stock_quantity'] -= info['stock_quantity']
                        info['trade_set'] = trade_set
                        if derived_trade_info[stock_name]['final_stock_quantity'] == 0:
                            trade_set += 1

            # To Determine Long or Short Position
            trade_position = ""
            # Get Maximum sets of trades
            max_trade_set_info = 1
            for info in trade_list:
                if info['stock_symbol'] == stock_name:
                    trade_set_info = info['trade_set']
                    if trade_set_info > max_trade_set_info:
                        max_trade_set_info = trade_set_info
            
            for trade_set_var in range(1, max_trade_set_info + 1): # +1 To Circumvent range function restriction
                trade_position_entry_time = '15:30:00'
                trade_position_exit_time  = '09:15:00'
                # Get Trade Entry Time and Trade Exit Time within a trade set
                for info in trade_list:
                    if info['stock_symbol'] == stock_name:
                        if info['trade_set'] == trade_set_var:
                            if datetime.strptime(info['trade_time'],'%H:%M:%S') < datetime.strptime(trade_position_entry_time, '%H:%M:%S'):
                                trade_position_entry_time = info['trade_time']
                            if datetime.strptime(info['trade_time'],'%H:%M:%S') > datetime.strptime(trade_position_exit_time, '%H:%M:%S'):
                                trade_position_exit_time = info['trade_time']
                # Based on the Trade Entry time determine the Trade Position
                for info in trade_list:
                    if info['stock_symbol'] == stock_name:
                        if info['trade_time'] == trade_position_entry_time:
                            if info['buy_or_sell'] == 'B':
                                trade_position = 'Long'
                            elif info['buy_or_sell'] == 'S':
                                trade_position = 'Short'
                # Update Trade Position to all trades under the trade set
                for info in trade_list:
                    if info['stock_symbol'] == stock_name:
                        if info['trade_set'] == trade_set_var:
                            info['trade_position'] = trade_position

            if derived_trade_info[stock_name]['final_stock_quantity'] == 0: # Buy and Sell are squared off
                derived_trade_info[stock_name]['trade_type'] = "Intraday Trading"
                derived_trade_info[stock_name]['trade_exit_date'] = trade_entry_date
                derived_trade_info[stock_name]['leverage'] = 5 # Default Leverage for Intraday Trading
            else:
                derived_trade_info[stock_name]['trade_type'] = "Swing Trading"
                derived_trade_info[stock_name]['trade_exit_date'] = None
                derived_trade_info[stock_name]['trade_exit_time'] = None
                derived_trade_info[stock_name]['leverage'] = 1
        # Append the Derived info into Trade List
        for stock_name in derived_trade_info:
            for info in trade_list:
                if info['stock_symbol'] == stock_name:
                    info['trade_entry_date'] = trade_entry_date
                    info['trade_entry_time'] = derived_trade_info[stock_name]['trade_entry_time']
                    info['trade_exit_date']  = derived_trade_info[stock_name]['trade_exit_date']
                    info['trade_exit_time'] = derived_trade_info[stock_name]['trade_exit_time']
                    info['final_stock_quantity'] = derived_trade_info[stock_name]['final_stock_quantity']
                    info['trade_type'] = derived_trade_info[stock_name]['trade_type']
                    info['leverage'] = derived_trade_info[stock_name]['leverage']

        remove(stock_pdf_file_name_path)
        remove(stock_text_file_name_path)
        return jsonify({'data': trade_list, 'fees': fees_info, 'message': 'Successfully uploaded the Stock PDF File and Parsed the File.','status': 'Success'})
    except Exception as e:
        return jsonify({'data': None, 'message': repr(e), 'status': 'Failed'})