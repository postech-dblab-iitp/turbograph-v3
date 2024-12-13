import requests

def test_execute_s62():
    url = "http://localhost:6543/execute-s62"  # Flask server endpoint
    query = """
    MATCH (item:LINEITEM)
    WHERE item.L_SHIPDATE <= date('1998-08-25')
    RETURN
        item.L_RETURNFLAG AS ret_flag,
        item.L_LINESTATUS AS line_stat,
        sum(item.L_QUANTITY) AS sum_qty,
        sum(item.L_EXTENDEDPRICE) AS sum_base_price,
        sum(item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT)) AS sum_disc_price,
        sum(item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT) * (1 + item.L_TAX)) AS sum_charge,
        avg(item.L_QUANTITY) AS avg_qty,
        avg(item.L_EXTENDEDPRICE) AS avg_price,
        avg(item.L_DISCOUNT) AS avg_disc,
        COUNT(*) AS count_order
    ORDER BY
        ret_flag,
        line_stat;
    """
    payload = {"query": query}

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()  # Raise an exception for HTTP errors

        data = response.json()

        print("Query Execution Results:")
        print(f"Elapsed Time: {data.get('elapsed_time', 'N/A')} ms")
        print(f"Result Set Size: {data.get('result_set_size', 0)} rows")
        
        print(data)

    except requests.exceptions.RequestException as e:
        print(f"HTTP Request failed: {e}")
    except ValueError as ve:
        print(f"Response parsing failed: {ve}")

if __name__ == "__main__":
    test_execute_s62()
