import pandas as pd
import numpy as np
import sys
import functools as ft

#==============================================================================================================
def create_dataframe(file_name,file_path):

    try:
        if file_name == 'carier_details':
            columns = ['product_id','description','supplier','carier_number']
            daraframe = pd.read_excel(file_path)[columns]
            daraframe.rename({'carier_number':'carrier_number'},axis=1,inplace=True)
            daraframe['product_id'] = daraframe.product_id.str.replace(' ', '')

        elif file_name == 'carrier_price_details':
            columns = ['carrier_number','valid_from','valid_to','price','currency']
            daraframe = pd.read_excel(file_path)[columns]

        elif file_name == 'currency':
            columns = ['currency','value']
            daraframe = pd.read_excel(file_path)[columns]

        elif file_name == 'customer_details':
            columns = ['customer_id','Name']
            daraframe = pd.read_excel(file_path)[columns]

        elif file_name == 'order_details':
            columns = ['order_id','user_id','product_id','quantity','date']
            daraframe = pd.read_excel(file_path)[columns]
            daraframe['product_id'] = daraframe.product_id.str.replace(' ', '')

        elif file_name == 'parameter':
            daraframe = pd.read_excel(file_path)

        elif file_name == 'product_details':
            columns = ['product_id','partial_naming','category']
            daraframe = pd.read_excel(file_path)[columns]
            daraframe['product_id'] = daraframe.product_id.str.replace(' ', '')
        else:
            print('Invalid file '+file_name)
            daraframe = pd.DataFrame()

    except Exception as e:
        daraframe = pd.DataFrame()
        print('Error: '+str(e))
    
    return daraframe

#==============================================================================================================

def parameter_data(df_para):
    columns_list = ['category' ,'percentage']
    df_package,df_tooling,df_discount = pd.DataFrame(columns=columns_list),pd.DataFrame(columns=columns_list),pd.DataFrame(columns=columns_list)
    df_final = pd.DataFrame(columns=['category' ,'packaing_percentage','tooling_percentage','discount_percentage'])
    # Creating the subset w.r.t different discount
    try:

        tooling_index = df_para.index[df_para['packaging-cost'].str.contains('Tooling',na=False)]
        discount_index = df_para.index[df_para['packaging-cost'].str.contains('Discount',na=False)]

        if len(tooling_index) > 0:
            df_tooling = df_para[tooling_index.tolist()[0]+1:discount_index.tolist()[0]][columns_list]
            df_package = df_para[:tooling_index.tolist()[0]][columns_list]

        if len(discount_index) > 0:
            df_discount = df_para[discount_index.tolist()[0]+1:][columns_list]

        df_package.dropna(inplace=True)
        df_tooling.dropna(inplace=True)
        df_discount.dropna(inplace=True)

    except Exception as e:
        print('Error: '+str(e))

    # Merging subset
    try:
        df_package.rename({'percentage':'packaing_percentage'},axis=1,inplace=True)
        df_tooling.rename({'percentage':'tooling_percentage'},axis=1,inplace=True)
        df_discount.rename({'percentage':'discount_percentage'},axis=1,inplace=True)

        df_final = ft.reduce(lambda left, right: pd.merge(left, right, on='category'), [df_package,df_tooling,df_discount])
        avg_discounts = [df_final['packaing_percentage'].mean(),df_final['tooling_percentage'].mean(),df_final['discount_percentage'].mean()]

    except Exception as e:
        print('Error: '+str(e))
        
    return df_final,avg_discounts

#==============================================================================================================

def join_table(typeofjoin,l_table,r_table,l_col,r_col):
    merge_df = pd.DataFrame()
    try:
        if l_col == r_col:
            merge_df = l_table.merge(r_table,how=typeofjoin,on=l_col)
        else:
            merge_df = l_table.merge(r_table,how=typeofjoin,left_on=l_col, right_on=r_col)
    except Exception as e:
        
        print('Error: '+str(e))
    
    return merge_df

#==============================================================================================================

def currency_conversion_avg_price(df_cpd,df_crcy):
    average_price = None
    merge_price_data = pd.DataFrame()
    try:
        merge_price_data = join_table('left',df_cpd,df_crcy,'currency','currency')
        '''
        #Since 'EUR' currency value is 1
        merge_price_data['price_in_EUR_backup'] = merge_price_data['price']/merge_price_data['value']
        '''
        
        # If 'EUR' currency value is not 1 then the generic approch
        EUR_index = df_crcy.index[df_crcy['currency'].str.contains('EUR',na=False)].tolist()[0]
        EUR_value = df_crcy.iloc[EUR_index]['value']
        
        merge_price_data['price_in_EUR'] = np.where(merge_price_data['currency']!= 'EUR',(merge_price_data['price']/merge_price_data['value'])*EUR_value,merge_price_data['price'])
        merge_price_data['carrier_number'] = merge_price_data['carrier_number'].astype(str)

        # print(merge_price_data)

        # get avaerage price basis on latest date
        df_latest_date = merge_price_data[merge_price_data.groupby('carrier_number').valid_to.transform('max') == merge_price_data['valid_to']]
        average_price = df_latest_date['price_in_EUR'].mean()

    except Exception as e:
        print('Error: '+str(e))

    return merge_price_data,average_price

#==============================================================================================================

def final_report(data,avg_price,avg_dis,na_type,dup_type,filename):
    f_columns = ['User ID','User name','Order Id','Product Id','Product Name','Product Category','Quantity','Currency']
    r_columns = ['user_id','Name','order_id','product_id','description','category','quantity','currency']

    try:
        if na_type == 'no_null':
            data = data.dropna()
            data = data[(data['date'] > data['valid_from']) & (data['date'] < data['valid_to'])]

        elif na_type == 'with_null':
            data1 = data[data['price'].notnull()]
            data2 = data[~data['price'].notnull()]
            data1 = data1[(data1['date'] > data1['valid_from']) & (data1['date'] < data1['valid_to'])]
            #Drop duplicates
            if dup_type == 'no_dup':
                data = pd.concat([data1,data2]).drop_duplicates()
            else:
                data = pd.concat([data1,data2])
            
            # Fill blank values with average value
            data =  data.fillna(value={'price_in_EUR':avg_price,'packaing_percentage':avg_dis[0],'tooling_percentage':avg_dis[1],'discount_percentage':avg_dis[2]})

        data['currency'] = 'EUR'
        data['Actual Price(EUR)'] = (data['price_in_EUR'] + data['price_in_EUR']*data['packaing_percentage'] +data['price_in_EUR']*data['tooling_percentage'])*data['quantity']
        data['Discounted Price(EUR)'] = data['Actual Price(EUR)'] - (data['Actual Price(EUR)']*data['discount_percentage'])
        data['Savings(%)'] = data['discount_percentage']*100

        # data['Savings(%)2'] = np.where((data['Actual Price(EUR)']!= 0) & (data['Discounted Price(EUR)']!=0),((data['Actual Price(EUR)'] - data['Discounted Price(EUR)'])/(data['Actual Price(EUR)'])) * 100,data['Actual Price(EUR)'])
        data.rename(dict(zip(r_columns,f_columns)),axis=1,inplace=True)
        data = data[f_columns+['Actual Price(EUR)','Discounted Price(EUR)','Savings(%)']]
        data.sort_values(by=['User ID','User name'],inplace=True)
        data.to_csv(filename,index=False)

    except Exception as e:
        data = pd.DataFrame()
        print('Error: '+str(e))

    return data

# --------------------------------------------- main program starts --------------------------------------------- #
if __name__== "__main__" :

    # All files Folder path
    folder_path = r'C:\\Users\\LENOVO\\Desktop\\MyStudy\\MyPractice\\Benz\\data\\data'

    #==============================================================================================================
    # All files
    carier_details = folder_path + '\\carier_details.xlsx'
    carrier_price_details = folder_path + '\\carrier_price_details.xlsx'
    currency = folder_path + '\\currency.xlsx'
    customer_details = folder_path + '\\customer_details.xlsx'
    order_details = folder_path + '\\order_details.xlsx'
    parameter = folder_path + '\\parameter.xlsx'
    product_details = folder_path + '\\product_details.xlsx'

    #==============================================================================================================
    # Creating a Dataframe for all Excel data
    df_cd = create_dataframe('carier_details',carier_details)
    if df_cd.empty:
        print('Invalid details in carier_details excel file')
        sys.exit(1)

    df_cpd = create_dataframe('carrier_price_details',carrier_price_details)
    if df_cpd.empty:
        print('Invalid details in carrier_price_details excel file')
        sys.exit(1)

    df_crcy = create_dataframe('currency',currency)
    if df_crcy.empty:
        print('Invalid details in currency excel file')
        sys.exit(1)

    df_cust_d = create_dataframe('customer_details',customer_details)
    if df_cust_d.empty:
        print('Invalid details in customer_details excel file')
        sys.exit(1)

    df_od = create_dataframe('order_details',order_details)
    if df_od.empty:
        print('Invalid details in order_details excel file')
        sys.exit(1)

    df_para = create_dataframe('parameter',parameter)
    if df_para.empty:
        print('Invalid details in parameter excel file')
        sys.exit(1)
    else:
        #creating the clean parameter percentage dataset and getting average discount values
        df_para,average_discounts = parameter_data(df_para)
        if df_para.empty:
            print('Invalid details in parameter excel file')
            sys.exit(1)

    df_pd = create_dataframe('product_details',product_details)
    if df_pd.empty:
        print('Invalid details in product_details excel file')
        sys.exit(1)

    #==============================================================================================================
    # Currency conversion
    df_price_details,average_price = currency_conversion_avg_price(df_cpd,df_crcy)
    if df_price_details.empty:
        print('Somethging went wrong while doing the currency conversion')
        sys.exit(1)

    #==============================================================================================================
    # Merging operation starts
    merge_cust_od = join_table('left',df_od,df_cust_d,'user_id','customer_id').drop(['customer_id'], axis=1)
    merge_cd_pd = join_table('left',df_cd,df_pd,'product_id','product_id')
    merge_cust_od_merge_cd = join_table('left',merge_cust_od,merge_cd_pd,'product_id','product_id')
    merge_cust_od_merge_cd_merge_para = join_table('left',merge_cust_od_merge_cd,df_para,'category','category')
    combined_df = join_table('left',merge_cust_od_merge_cd_merge_para,df_price_details,'carrier_number','carrier_number')

    #==============================================================================================================
    #creating the final report by cleaning up all the null values from the dataset
    final_dataset1 = final_report(combined_df,None,None,'no_null',None,'Report_avaiable_data.csv')

    #creating the final report by using average values for null columns and having only unique records
    final_dataset2 = final_report(combined_df,average_price,average_discounts,'with_null','no_dup','Report_No_Duplicate_data.csv')

    #creating the final report by using average values for null columns and all records
    final_dataset3 = final_report(combined_df,average_price,average_discounts,'with_null','with_dup','Report_with_Duplicate_data.csv')
