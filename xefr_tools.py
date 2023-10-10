def persist_data(curl_command,schema_name):
    """Run the curl command and save the output to a file"""
    logger.debug('persist_data: %s', locals())

    curl_output = subprocess.check_output(curl_command, shell=True)

    output_file = os.path.join(XEFR_DATA_DIR,schema_name +'.csv')

    with open(output_file, 'wb') as file:
        file.write(curl_output)
    try:
        df = pd.read_csv(output_file)

        if schema_name in sort_orders: # Require data to be sorted
            utilities.data_frame_sort_and_save(df,output_file,sort_orders[schema_name],logger)

        record_count = len(df)
       # df.to_csv(output_file,index=False,encoding='utf-8') # ensure in same format for reconciliations
    except pd.errors.EmptyDataError:
        record_count = 0

    print(f"Data saved to {output_file} ({record_count} rows)\n")

    if __name__ == '__main__':
        pass