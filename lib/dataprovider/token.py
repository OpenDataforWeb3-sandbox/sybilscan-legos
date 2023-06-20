
def get_token_prices(tokens):
    """
    retrieve pricing information for the given token symbols
    """
    logger.info("executing query: {}".format(sys._getframe().f_code.co_name))
    
    tokens_concatenated = ",".join(["'" + t + "'" for t in tokens])

    query_token_prices = """
    SELECT
        DATE_TRUNC('DAY', HOUR) day,
        symbol,
        MEDIAN(price) AS price_usd
    FROM
        ethereum.core.fact_hourly_token_prices
    WHERE
        symbol IN (%s)
    GROUP BY
        day,
        symbol
    """ % (tokens_concatenated)

    return run_query(query_token_prices)