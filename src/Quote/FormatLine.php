<?php

namespace ClickHouseDB\Quote;

class FormatLine
{
    /**
     *
     * @var array
     */
    private static $strict=[];

    /**
     * Format
     */
    private static function strictQuote(string $format) : StrictQuoteLine
    {
        if (empty(self::$strict[$format]))
        {
            self::$strict[$format]=new StrictQuoteLine($format);
        }
        return self::$strict[$format];
    }

    /**
     * Array in a string for a query Insert
     *
     * @param array $row
     * @return string
     */
    public static function Insert(Array $row)
    {
        return self::strictQuote('Insert')->quoteRow($row);
    }

    /**
     * Array to TSV
     *
     * @param array $row
     * @return string
     */
    public static function TSV(Array $row)
    {
        return self::strictQuote('TSV')->quoteRow($row);
    }

    /**
     * Array to CSV
     *
     * @param array $row
     * @return string
     */
    public static function CSV(Array $row)
    {
        return self::strictQuote('CSV')->quoteRow($row);
    }
}
