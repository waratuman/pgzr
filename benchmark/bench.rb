# frozen_string_literal: true

# Benchmark: pg_replication (Ruby) WAL message throughput
#
# Usage: ruby benchmark/bench.rb [row_count]
#
# This script:
#   1. Creates a benchmark database, table, and replication slot
#   2. Inserts N rows
#   3. Records the end WAL LSN
#   4. Consumes all WAL messages up to that LSN, timing the consumption
#   5. Cleans up

require "pg"

pg_replication_lib = File.expand_path("../../pg_replication/lib", File.dirname(__FILE__))
$LOAD_PATH.unshift(pg_replication_lib)
require "pg_replication"

DB_NAME = "pgzr_bench"
SLOT_NAME = "pgzr_bench_slot"
ROW_COUNT = (ARGV[0] || 10_000).to_i

def run_sql(db, sql)
  conn = PG.connect(dbname: db)
  conn.exec(sql)
ensure
  conn&.finish
end

def setup
  # Create database
  begin
    conn = PG.connect(dbname: "postgres")
    conn.exec("CREATE DATABASE #{DB_NAME}")
  rescue PG::DuplicateDatabase
    # already exists
  ensure
    conn&.finish
  end

  run_sql(DB_NAME, "CREATE TABLE IF NOT EXISTS bench (id serial PRIMARY KEY, val text)")
  run_sql(DB_NAME, "SELECT pg_drop_replication_slot('#{SLOT_NAME}')") rescue nil
  run_sql(DB_NAME, "SELECT pg_create_logical_replication_slot('#{SLOT_NAME}', 'test_decoding')")
end

def teardown
  run_sql(DB_NAME, "SELECT pg_drop_replication_slot('#{SLOT_NAME}')") rescue nil
  begin
    conn = PG.connect(dbname: "postgres")
    conn.exec("DROP DATABASE IF EXISTS #{DB_NAME} WITH (FORCE)")
  ensure
    conn&.finish
  end
end

def insert_rows(n)
  conn = PG.connect(dbname: DB_NAME)
  conn.exec("INSERT INTO bench (val) SELECT 'row_' || g FROM generate_series(0, #{n - 1}) g")
ensure
  conn&.finish
end

def get_wal_lsn
  conn = PG.connect(dbname: DB_NAME)
  lsn_str = conn.exec("SELECT pg_current_wal_lsn()").getvalue(0, 0)
  parse_lsn(lsn_str)
ensure
  conn&.finish
end

def parse_lsn(str)
  high, low = str.split("/")
  (high.to_i(16) << 32) | low.to_i(16)
end

def format_lsn(lsn)
  "%X/%X" % [lsn >> 32, lsn & 0xFFFFFFFF]
end

# Main
puts "pg_replication (Ruby) Benchmark"
puts "=" * 40
puts "Rows: #{ROW_COUNT}"
puts

begin
  setup
  insert_rows(ROW_COUNT)
  end_lsn = get_wal_lsn
  puts "End LSN: #{format_lsn(end_lsn)}"

  msg_count = 0

  start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  repl = PG::Replicator.new(
    dbname: DB_NAME,
    slot: SLOT_NAME,
    end_position: format_lsn(end_lsn),
    replication_options: { "include-timestamp" => "on" }
  ) do |msg|
    msg_count += 1
  end

  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time

  puts "Messages: #{msg_count}"
  puts "Time: %.4f seconds" % elapsed
  puts "Throughput: %.0f messages/second" % (msg_count / elapsed)
  puts
ensure
  teardown
end
