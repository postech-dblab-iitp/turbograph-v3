#include "api/c-api/s62.h"
#include <stdio.h>

int main() {
    s62_state state = s62_connect("/data/tpch/sf1-tmax/");
    printf("s62_connect() done\n");
    printf("state: %d\n", state);

    state = s62_is_connected();
    printf("s62_is_connected() done\n");
    printf("state: %d\n", state);

    s62_metadata* metadata;
    s62_num_metadata num_metadata = s62_get_metadata_from_catalog(NULL, false, false, &metadata);
    printf("s62_get_metadata_from_catalog() done\n");
    printf("num_metadata: %ld\n", num_metadata);
    
    // Iterate over linked list, and print label name and type
    s62_metadata* current = metadata;
    while (current != NULL) {
        printf("label: %s, type: %d\n", current->label_name, current->type);
        current = current->next;
    }

    // For each label, get properties
    s62_metadata* current_metadata = metadata;
    for (int i = 0; i < num_metadata; i++) {
        printf("Getting properties for label: %s, type: %d\n", current_metadata->label_name, current_metadata->type);
        s62_property* properties;
        s62_num_properties num_properties = s62_get_property_from_catalog(current_metadata->label_name, current_metadata->type, &properties);
        printf("s62_get_properties_from_catalog() done\n");
        printf("num_properties: %ld\n", num_properties);

        s62_property* current_property = properties;
        while (current_property != NULL) {
            printf("order: %d, property_name: %s, property_type: %d, property_sql_type: %s, precision: %d, scale: %d\n", 
                current_property->order, current_property->property_name, current_property->property_type, current_property->property_sql_type, current_property->precision, current_property->scale);
            current_property = current_property->next;
        }

        s62_close_property(properties);
        current_metadata = current_metadata->next;
    }

    s62_close_metadata(metadata);

    // Prepare query
    s62_prepared_statement* prep_stmt = s62_prepare("MATCH (item:LINEITEM) \
		WHERE item.L_SHIPDATE <= $ext \
		RETURN \
			item.L_RETURNFLAG AS ret_flag, \
			item.L_LINESTATUS AS line_stat, \ 
			sum(item.L_QUANTITY) AS sum_qty, \
			sum(item.L_EXTENDEDPRICE) AS sum_base_price, \
			sum(item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT)) AS sum_disc_price, \
			sum(item.L_EXTENDEDPRICE*(1 - item.L_DISCOUNT)*(1 + item.L_TAX)) AS sum_charge, \
			avg(item.L_QUANTITY) AS avg_qty, \
			avg(item.L_EXTENDEDPRICE) AS avg_price, \
			avg(item.L_DISCOUNT) AS avg_disc, \
			COUNT(*) AS count_order \
		ORDER BY \
			ret_flag, \
			line_stat;");


    printf("s62_prepare_query() done\n");

    printf("Prepared statement original query: %s\n", prep_stmt->query);
    printf("Prepared statement plan: \n%s\n", prep_stmt->plan);
    printf("Prepared statement num_properties: %ld\n", prep_stmt->num_properties);
    s62_property* current_property = prep_stmt->property;
    while (current_property != NULL) {
        if (current_property->label_name == NULL) {
            printf("order: %d, label_type: %d, property_name: %s, property_type: %d, property_sql_type: %s, precision: %d, scale: %d\n", 
                current_property->order, current_property->label_type, current_property->property_name, current_property->property_type, current_property->property_sql_type, current_property->precision, current_property->scale);
        }
        else {
            printf("order: %d, label_name: %s, label_type: %d, property_name: %s, property_type: %d, property_sql_type: %s, precision: %d, scale: %d\n", 
                current_property->order, current_property->label_name, current_property->label_type, current_property->property_name, current_property->property_type, current_property->property_sql_type, current_property->precision, current_property->scale);
        }
        current_property = current_property->next;
    }

    // Bind value
    // Create s62_decimal value
    s62_bind_date_string(prep_stmt, 1, "1998-08-25");
    printf("s62_bind_value() done\n");

    // Execute query
    s62_resultset_wrapper *resultset_wrapper;
    s62_num_rows rows = s62_execute(prep_stmt, &resultset_wrapper);
    printf("s62_execute_query() done\n");
    printf("rows: %ld, num_properties: %ld, cursor: %ld\n", rows, resultset_wrapper->result_set->num_properties, resultset_wrapper->cursor);
    printf("Prepared statement plan after execution: \n%s\n", prep_stmt->plan);

    // Fetch results
    while (s62_fetch_next(resultset_wrapper) != S62_END_OF_RESULT) {
        s62_string ret_flag = s62_get_varchar(resultset_wrapper, 0);
        printf("ret_flag: %s\n", ret_flag);


        // uint64_t customer_id = s62_get_id(resultset_wrapper, 0);
        // uint64_t customer_custkey = s62_get_uint64(resultset_wrapper, 1);
        // s62_string customer_name = s62_get_varchar(resultset_wrapper, 2);
        // s62_string customer_address = s62_get_varchar(resultset_wrapper, 3);
        // uint64_t customer_nationkey = s62_get_uint64(resultset_wrapper, 4);
        // s62_string customer_phone = s62_get_varchar(resultset_wrapper, 5);
        // s62_string customer_acctbal = s62_decimal_to_string(s62_get_decimal(resultset_wrapper, 6));
        // s62_string customer_mktsegment = s62_get_varchar(resultset_wrapper, 7);
        // s62_string customer_comment = s62_get_varchar(resultset_wrapper, 8);

        // printf("customer_id: %ld, customer_custkey: %ld, customer_name: %s, customer_address: %s, customer_nationkey: %ld, customer_phone: %s, customer_acctbal: %s, customer_mktsegment: %s, customer_comment: %s\n", 
        //     customer_id, customer_custkey, customer_name.data, customer_address.data, customer_nationkey, customer_phone.data, customer_acctbal.data, customer_mktsegment.data, customer_comment.data);

        // uint64_t nation_id = s62_get_id(resultset_wrapper, 9);
        // uint64_t nation_key = s62_get_uint64(resultset_wrapper, 10);
        // s62_string nation_name = s62_get_varchar(resultset_wrapper, 11);
        // uint64_t nation_regionkey = s62_get_uint64(resultset_wrapper, 12);
        // s62_string nation_comment = s62_get_varchar(resultset_wrapper, 13);

        // printf("nation_id: %ld, nation_key: %ld, nation_name: %s, nation_regionkey: %ld, nation_comment: %s\n", nation_id, nation_key, nation_name.data, nation_regionkey, nation_comment.data);

        // uint64_t cust_belong_to_id = s62_get_id(resultset_wrapper, 14);
        // uint64_t cust_belong_to_custkey = s62_get_uint64(resultset_wrapper, 15);
        // uint64_t cust_belong_to_nationkey = s62_get_uint64(resultset_wrapper, 16);

        // printf("cust_belong_to_id: %ld, cust_belong_to_custkey: %ld, cust_belong_to_nationkey: %ld\n", cust_belong_to_id, cust_belong_to_custkey, cust_belong_to_nationkey);

    }
    printf("s62_fetch() done\n");

    s62_close_resultset(resultset_wrapper);
    printf("s62_close_resultset() done\n");

    s62_close_prepared_statement(prep_stmt);
    printf("s62_close_prepared_statement() done\n");

    s62_disconnect();

    return 0;
}
