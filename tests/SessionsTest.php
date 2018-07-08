<?php

namespace ClickHouseDB\Tests;

use ClickHouseDB\Exception\DatabaseException;
use PHPUnit\Framework\TestCase;

/**
 * Class ClientTest
 * @group ClientTest
 */
final class SessionsTest extends TestCase
{
    use WithClient;

    /**
     * @throws Exception
     */
    public function setUp()
    {
        date_default_timezone_set('Europe/Moscow');

        $this->client->ping();
    }

    public function testCreateTableTEMPORARYNoSession()
    {
        $this->expectException(DatabaseException::class);

        $this->client->write('DROP TABLE IF EXISTS phpunti_test_xxxx');
        $this->client->write('
            CREATE TEMPORARY TABLE IF NOT EXISTS phpunti_test_xxxx (
                event_date Date DEFAULT toDate(event_time),
                event_time DateTime,
                url_hash String,
                site_id Int32,
                views Int32
            ) ENGINE = TinyLog
        ');
    }

    public function testUseSession() : void
    {
        self::assertNull($this->client->getSessionId());
        $this->client->useSession();
        self::assertStringMatchesFormat('%s', $this->client->getSessionId());
    }


    public function testCreateTableTEMPORARYWithSessions()
    {
        // make two session tables
        $table_name_A = 'phpunti_test_A_abcd_' . time();
        $table_name_B = 'phpunti_test_B_abcd_' . time();

        // make new session id
        $sessionIdA = $this->client->useSession()->getSessionId();

        // create table in session A
        $this->client->write(' CREATE TEMPORARY TABLE IF NOT EXISTS ' . $table_name_A . ' (number UInt64)');
        $this->client->write('INSERT INTO ' . $table_name_A . ' SELECT number FROM system.numbers LIMIT 30');

        $st = $this->client->select('SELECT round(avg(number),1) as avs FROM ' . $table_name_A);
        // check
        $this->assertEquals(14.5, $st->fetchOne('avs'));

        // reconnect + reinit session

        // create table in session B
        $sessionIdB = $this->client->useSession()->getSessionId();

        $this->client->write(' CREATE TEMPORARY TABLE IF NOT EXISTS ' . $table_name_B . ' (number UInt64)');

        $this->client->write('INSERT INTO ' . $table_name_B . ' SELECT number*1234 FROM system.numbers LIMIT 30');

        $st = $this->client->select('SELECT round(avg(number),1) as avs FROM ' . $table_name_B);
        // check
        $this->assertEquals(17893, $st->fetchOne('avs'));




        // Reuse session A

        $this->client->useSession($sessionIdA);

        $st = $this->client->select('SELECT round(avg(number),1) as avs FROM ' . $table_name_A);
        $this->assertEquals(14.5, $st->fetchOne('avs'));


        // Reuse session B

        $this->client->useSession($sessionIdB);


        $st = $this->client->select('SELECT round(avg(number),1) as avs FROM ' . $table_name_B);
        // check
        $this->assertEquals(17893, $st->fetchOne('avs'));
    }
}
