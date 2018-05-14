<?php

namespace ClickHouseDB\Query;

use ClickHouseDB\Exception\QueryException;
use function dirname;
use function is_file;
use function is_writable;
use function unlink;

class WriteToFile
{
    /**
     *
     */
    const FORMAT_TabSeparated          = 'TabSeparated';
    const FORMAT_TabSeparatedWithNames = 'TabSeparatedWithNames';
    const FORMAT_CSV                   = 'CSV';

    private $support_format=['TabSeparated','TabSeparatedWithNames','CSV'];
    /**
     * @var string
     */
    private $file_name = null;

    /**
     * @var string
     */
    private $format='CSV';

    /** @var bool */
    private $gzip = false;

    public function __construct(string $fileName, bool $overwrite = true, ?string $format = null)
    {
        if (!$fileName) {
            throw new QueryException('Bad file path');
        }

        if (is_file($fileName)) {
            if (!$overwrite) {
                throw new QueryException('File exists: ' . $fileName);
            }

            if (!unlink($fileName)) {
                throw new QueryException('Can`t delete: ' . $fileName);
            }
        }

        $dir = dirname($fileName);

        if (!is_writable($dir)) {
            throw new QueryException('Can`t writable dir: ' . $dir);
        }

        if ($format !== null) {
            $this->setFormat($format);
        }

        $this->file_name = $fileName;
    }

    public function getGzip() : bool
    {
        return $this->gzip;
    }

    public function setGzip(bool $flag) : void
    {
        $this->gzip = $flag;
    }

    public function setFormat(string $format)
    {
        if (!in_array($format,$this->support_format))
        {
            throw new QueryException('Unsupport format: ' . $format);
        }
        $this->format = $format;
    }
    /**
     * @return int
     */
    public function size()
    {
        return filesize($this->file_name);
    }

    /**
     * @return string
     */
    public function fetchFile()
    {
        return $this->file_name;
    }

    /**
     * @return string
     */
    public function fetchFormat()
    {
        return $this->format;
    }

}
