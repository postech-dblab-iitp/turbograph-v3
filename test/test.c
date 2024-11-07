#include "s62.h"
#include <stdio.h>

int main() {
    s62_state state = s62_connect("/data/tpch/sf1/");
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
    s62_prepared_statement* prep_stmt = s62_prepare("MATCH (pa:PART)-[p:PARTSUPP]->(s:SUPPLIER)-[:SUPP_BELONG_TO]->(n:NATION)-[:IS_LOCATED_IN]->(r:REGION)  WHERE pa.P_SIZE = 43 and pa.P_TYPE CONTAINS 'COPPER' and r.R_NAME = 'AMERICA'  WITH pa, p.PS_SUPPLYCOST AS PS_SUPPLYCOST, s.S_ACCTBAL AS S_ACCTBAL,s.S_NAME AS S_NAME, s.S_ADDRESS AS S_ADDRESS,s.S_PHONE AS S_PHONE,s.S_COMMENT AS S_COMMENT, n.N_NAME AS N_NAME MATCH (pa)-[p2:PARTSUPP]->(:SUPPLIER)-[:SUPP_BELONG_TO]->(:NATION)-[:IS_LOCATED_IN]->(r2:REGION) WHERE r2.R_NAME = 'AMERICA'  WITH pa.P_PARTKEY AS P_PARTKEY, pa.P_MFGR AS P_MFGR, PS_SUPPLYCOST, S_ACCTBAL, S_NAME, S_ADDRESS, S_PHONE, S_COMMENT, N_NAME, min(p2.PS_SUPPLYCOST) as minvalue WHERE PS_SUPPLYCOST = minvalue RETURN S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT ORDER BY S_ACCTBAL desc,N_NAME,S_NAME,P_PARTKEY LIMIT 100;");

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
    s62_hugeint hint_value = {100010, 0};
    s62_decimal dec_value = {6, 2, hint_value};
    s62_bind_decimal(prep_stmt, 1, dec_value);
    printf("s62_bind_value() done\n");

    // Execute query
    s62_resultset_wrapper *resultset_wrapper;
    s62_num_rows rows = s62_execute(prep_stmt, &resultset_wrapper);
    printf("s62_execute_query() done\n");
    printf("rows: %ld, num_properties: %ld, cursor: %ld\n", rows, resultset_wrapper->result_set->num_properties, resultset_wrapper->cursor);
    printf("Prepared statement plan after execution: \n%s\n", prep_stmt->plan);

    // Fetch results
    // while (s62_fetch_next(resultset_wrapper) != S62_END_OF_RESULT) {
    //     s62_string ret_flag_value = s62_get_varchar(resultset_wrapper, 0);
    //     s62_string line_stat_value = s62_get_varchar(resultset_wrapper, 1);
    //     s62_hugeint sum_qty_value = s62_get_hugeint(resultset_wrapper, 2);
    //     s62_string sum_base_price_value = s62_decimal_to_string(s62_get_decimal(resultset_wrapper, 3));
    //     s62_string sum_disc_price_value = s62_decimal_to_string(s62_get_decimal(resultset_wrapper, 4));
    //     s62_string sum_charge_value = s62_decimal_to_string(s62_get_decimal(resultset_wrapper, 5));
    //     double avg_qty_value = s62_get_double(resultset_wrapper, 6);
    //     double avg_price_value = s62_get_double(resultset_wrapper, 7);
    //     double avg_disc_value = s62_get_double(resultset_wrapper, 8);
    //     int64_t count_order_value = s62_get_int64(resultset_wrapper, 9);
    //     printf("ret_flag: %s, line_stat: %s, sum_qty: %ld%ld, sum_base_price: %s, sum_disc_price: %s, sum_charge: %s, avg_qty: %f, avg_price: %f, avg_disc: %f, count_order: %ld\n", 
    //             ret_flag_value.data, line_stat_value.data, sum_qty_value.upper, sum_qty_value.lower, 
    //             sum_base_price_value.data, 
    //             sum_disc_price_value.data,
    //             sum_charge_value.data, 
    //             avg_qty_value, avg_price_value, avg_disc_value, count_order_value);
    // }
    // printf("s62_fetch() done\n");

    s62_close_resultset(resultset_wrapper);
    printf("s62_close_resultset() done\n");

    s62_close_prepared_statement(prep_stmt);
    printf("s62_close_prepared_statement() done\n");

    s62_disconnect();

    return 0;
}
