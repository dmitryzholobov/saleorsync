using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Configuration;
using System.Data.SqlClient;
using System.Data;
using System.Web;
using System.Text.RegularExpressions;
using System.IO;
using System.IO.Compression;
using System.Threading;
using Npgsql;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace SaleorSync
{

    class ObdManager
    {
        private NpgsqlConnection mconn;
        private SqlConnection aconn;

        public ObdManager()
        {
            aconn = new SqlConnection(Settings.AzureConnectionString);
            mconn = new NpgsqlConnection(Settings.PostgreConnectionString);
            mconn.Open();
            aconn.Open();

            SqlCommand atcomm = new SqlCommand(@"   truncate table s_saleor_product
                                                    truncate table s_saleor_order
                                                    truncate table s_saleor_orderline
                                                    truncate table s_saleor_user_addresses
                                                    truncate table s_saleor_address
                                                    truncate table s_saleor_user
                                                    truncate table s_saleor_order_fulfillment
                                                    truncate table s_saleor_orderevent", aconn);
            atcomm.ExecuteNonQuery();


            CopyTable(@"SELECT id,name,description,price_amount,publication_date,updated_at,product_type_id,attributes,is_published,category_id,seo_description,
                seo_title,charge_taxes,weight,description_json,meta,private_meta,minimal_variant_price_amount,currency FROM product_product",
                "Insert into s_saleor_product Select * from @t", "t_saleor_product");
            CopyTable("SELECT id,user_id,address_id FROM account_user_addresses ",
                "Insert into s_saleor_user_addresses Select * from @t", "t_saleor_user_addresses");

            



            IncrementalCopyTable(@"SELECT id,is_superuser,email,is_staff,is_active,password,date_joined,last_login,default_billing_address_id,default_shipping_address_id,
                note,first_name,last_name,avatar,private_meta,meta,sso_id,updated FROM account_user",
                "Insert into s_saleor_user Select * from @t", "t_saleor_user", getLastUpdated("saleor_user"));

            IncrementalCopyTable(@"SELECT id,first_name,last_name,company_name,street_address_1,street_address_2,city,postal_code,country,country_area,
                phone,city_area,date_of_birth,sex,updated  FROM  account_address",
                "Insert into s_saleor_address Select * from @t", "t_saleor_address", getLastUpdated("saleor_address"));

            IncrementalCopyTable(@"SELECT id,created,tracking_client_id,user_email,token,billing_address_id,shipping_address_id,user_id,total_net_amount,discount_amount,
                discount_name,voucher_id,language_code,shipping_price_gross_amount,total_gross_amount,shipping_price_net_amount,status,shipping_method_name,
                shipping_method_id,display_gross_prices,translated_discount_name,customer_note,weight,checkout_token,currency,external_lab_id,updated FROM order_order",
               "Insert into s_saleor_order Select * from @t", "t_saleor_order", getLastUpdated("saleor_order"));


            IncrementalCopyTable(@"SELECT id,product_name,product_sku,quantity,unit_price_net_amount,unit_price_gross_amount,is_shipping_required,
                order_id,quantity_fulfilled,variant_id,tax_rate,translated_product_name,currency,translated_variant_name,variant_name,updated FROM order_orderline",
              "Insert into s_saleor_orderline Select * from @t", "t_saleor_orderline", getLastUpdated("saleor_orderline"));

            IncrementalCopyTable(@"SELECT id, date, type, order_id, user_id, parameters,updated FROM order_orderevent",
             "Insert into s_saleor_orderevent Select * from @t", "t_saleor_orderevent", getLastUpdated("saleor_orderevent"));

            IncrementalCopyTable(@"SELECT id, tracking_number, shipping_date, order_id, fulfillment_order, status, updated FROM order_fulfillment",
             "Insert into s_saleor_order_fulfillment Select * from @t", "t_saleor_order_fulfillment", getLastUpdated("saleor_order_fulfillment"));
            
            atcomm = new SqlCommand(@"exec sync_saleor", aconn);
            atcomm.CommandTimeout = 0;
            atcomm.ExecuteNonQuery();

        }

        private string getLastUpdated(string tableName)
        {
            SqlCommand comm = new SqlCommand(@"Select ISNULL(MAX(updated),'20190101') updated from "+ tableName, aconn);
            DateTime last_updated = (DateTime)comm.ExecuteScalar();
            return last_updated.ToString("yyyy-MM-dd HH:mm:ss");
        }

        private void IncrementalCopyTable(string selectCommand, string insertCommand, string typeName, string last_updated)
        {
            NpgsqlCommand mcomm = new NpgsqlCommand(selectCommand+" where updated>='"+last_updated+"'", mconn);
            DataSet ds = new DataSet();
            NpgsqlDataAdapter adp = new NpgsqlDataAdapter(mcomm);
            adp.Fill(ds);
            DataTable tbl = ds.Tables[0];

            SqlCommand cmd = new SqlCommand(insertCommand, aconn);
            SqlParameter tvpParam =
               cmd.Parameters.AddWithValue(
               "@t", tbl);
            tvpParam.SqlDbType = SqlDbType.Structured;
            tvpParam.TypeName = typeName;
            cmd.ExecuteNonQuery();
        }

        private void CopyTable(string selectCommand, string insertCommand, string typeName)
        {
            NpgsqlCommand mcomm = new NpgsqlCommand(selectCommand,mconn);
            DataSet ds = new DataSet();
            NpgsqlDataAdapter adp = new NpgsqlDataAdapter(mcomm);
            adp.Fill(ds);
            DataTable tbl = ds.Tables[0];

            SqlCommand cmd = new SqlCommand(insertCommand, aconn);
            SqlParameter tvpParam =
               cmd.Parameters.AddWithValue(
               "@t", tbl);
            tvpParam.SqlDbType = SqlDbType.Structured;
            tvpParam.TypeName = typeName;
            cmd.ExecuteNonQuery();
        }

    }

    class AppAction
    {
        public string deviceId;
        public string pid;
        public int eventid;
        public long timestamp;
        public string extra;
        public string fb_id;
    }
    class Settings
    {
        public static String AzureConnectionString
        {
            get { return ConfigurationManager.AppSettings["AzureConnectionString"]; }
        }
        public static String PostgreConnectionString
        {
            get { return ConfigurationManager.AppSettings["PostgreConnectionString"]; }
        }
     
        
    }

  
}
