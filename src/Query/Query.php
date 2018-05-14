<?php

namespace ClickHouseDB\Query;

use ClickHouseDB\Exception\QueryException;
use function sizeof;

class Query
{
    /**
     * @var string
     */
    protected $sql;

    /**
     * @var null
     */
    protected $format = null;

    /** @var Degeneration[] */
    private $degenerations = [];

    /**
     * @param Degeneration[] $degenerations
     */
    public function __construct(string $sql, array $degenerations = [])
    {
        if (!trim($sql))
        {
            throw new QueryException('Empty Query');
        }
        $this->sql = $sql;
        $this->degenerations=$degenerations;
    }

    public function setFormat(string $format) : void
    {
        $this->format = $format;
    }


    private function applyFormatQuery()
    {
        // FORMAT\s(\w)*$
        if (null === $this->format) return false;
        $supportFormats=
            "FORMAT\\s+TSV|FORMAT\\s+TSVRaw|FORMAT\\s+TSVWithNames|FORMAT\\s+TSVWithNamesAndTypes|FORMAT\\s+Vertical|FORMAT\\s+JSONCompact|FORMAT\\s+JSONEachRow|FORMAT\\s+TSKV|FORMAT\\s+TabSeparatedWithNames|FORMAT\\s+TabSeparatedWithNamesAndTypes|FORMAT\\s+TabSeparatedRaw|FORMAT\\s+BlockTabSeparated|FORMAT\\s+CSVWithNames|FORMAT\\s+CSV|FORMAT\\s+JSON|FORMAT\\s+TabSeparated";

        $matches=[];
        if (preg_match_all('%('.$supportFormats.')%ius',$this->sql,$matches)){

            // skip add "format json"
            if (isset($matches[0]))
            {
                $format=trim(str_ireplace('format','',$matches[0][0]));
                $this->format=$format;

            }
        }
        else {
            $this->sql = $this->sql . ' FORMAT ' . $this->format;
        }






    }
    public function getFormat()
    {

        return $this->format;
    }

    public function toSql() : string
    {
        if ($this->format !== null) {
            $this->applyFormatQuery();
        }

        if (sizeof($this->degenerations))
        {
            foreach ($this->degenerations as $degeneration)
            {
                if ($degeneration instanceof Degeneration) {
                    $this->sql=$degeneration->process($this->sql);
                }
            }
        }

        return $this->sql;
    }

    /**
     * @return string
     */
    public function __toString()
    {
        return $this->toSql();
    }
}
