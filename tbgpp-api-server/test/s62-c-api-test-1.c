#include "s62.h"
#include <stdio.h>

int main() {
    s62_state state = s62_connect("/data/tpch/tpch_demo/");
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
    s62_prepared_statement* prep_stmt = s62_prepare("MATCH (c:CUSTOMER)-[r:CUST_BELONG_TO]->(n:NATION) WHERE c.C_CUSTKEY < 1000 and c.C_NATIONKEY > 14 RETURN n, r, c limit 100");

    printf("s62_prepare_query() done\n");

    printf("Prepared statement original query: %s\n", prep_stmt->query);
    printf("Prepared statement plan: %s\n", prep_stmt->plan);
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
    s62_bind_uint64(prep_stmt, 1, 14);
    printf("s62_bind_value() done\n");

    // Execute query
    s62_resultset_wrapper *resultset_wrapper;
    s62_num_rows rows = s62_execute(prep_stmt, &resultset_wrapper);
    printf("s62_execute_query() done\n");
    printf("rows: %ld, num_properties: %ld, cursor: %ld\n", rows, resultset_wrapper->result_set->num_properties, resultset_wrapper->cursor);

    // Fetch results=
    while (s62_fetch_next(resultset_wrapper) != S62_END_OF_RESULT) {
        uint64_t n_id = s62_get_id(resultset_wrapper, 0);
        uint64_t n_nationkey = s62_get_uint64(resultset_wrapper, 1);
        s62_string n_name = s62_get_varchar(resultset_wrapper, 2);
        uint64_t r_id = s62_get_id(resultset_wrapper, 4);
        uint64_t r_sid = s62_get_uint64(resultset_wrapper, 5);
        uint64_t r_tid = s62_get_uint64(resultset_wrapper, 6);

        printf("n_id: %ld, n_nationkey: %ld, n_name: %s, r_id: %ld, r_sid: %ld, r_tid: %ld\n", 
            n_id, n_nationkey, n_name.data, r_id, r_sid, r_tid);
    }
    printf("s62_fetch() done\n");

    s62_close_resultset(resultset_wrapper);
    printf("s62_close_resultset() done\n");

    s62_close_prepared_statement(prep_stmt);
    printf("s62_close_prepared_statement() done\n");

    s62_disconnect();

    return 0;
}
