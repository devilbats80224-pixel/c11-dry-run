"""C11-only state boundary and native Freqtrade/CCXT configuration verification."""
import fcntl
from importlib.metadata import version
import os
from pathlib import Path
import sqlite3
import stat

from source_gate import require

FREQTRADE = '2026.10'
CCXT = '4.5.90'
RUNTIME_IMAGE = 'sha256:0cc4b6dc9b16e0516d27efae1765e925da7d6aea5c8f3c3825930ffe3098d7c7'
ROOT = Path('/c11-paper')
TRADE_DB = ROOT / 'trades.c11.paper.sqlite'
AUDIT_DB = ROOT / 'startup.c11.sqlite'
LOCK = ROOT / 'c11-paper.writer.lock'
EXCHANGE = dict(name='binance', enable_ws=False)
CREDENTIALS = ('apiKey', 'secret', 'password', 'uid', 'accountId', 'walletAddress', 'privateKey')


def dry_config():
    from freqtrade.enums import RunMode
    return dict(dry_run=True, runmode=RunMode.DRY_RUN, trading_mode='futures', margin_mode='isolated',
                exchange=dict(EXCHANGE), db_url='sqlite:///' + str(TRADE_DB),
                api_server={'enabled': False}, telegram={'enabled': False})


def check_config(config):
    expected = dry_config()
    require(config.keys() == expected.keys() and config.get('dry_run') is True
            and config == expected and type(config['exchange']['enable_ws']) is bool
            and config['api_server']['enabled'] is False and config['telegram']['enabled'] is False,
            'C11_FT_DRY_CONFIG_REFUSED')
    require(not any(key.startswith('FREQTRADE__') for key in os.environ), 'C11_FT_ENV_OVERRIDE')
    require(not (Path('/run/openmarket-credential/key').exists()), 'C11_UNEXPECTED_SOURCE_CREDENTIAL_MOUNT')


def verify_native_exchange():
    from freqtrade.resolvers import ExchangeResolver
    require(version('freqtrade') == FREQTRADE and version('ccxt') == CCXT, 'C11_RUNTIME_VERSION_PIN')
    config = dry_config()
    check_config(config)
    # Validate=False avoids market downloads. Actual Freqtrade builds both CCXT
    # clients. Binance's private account initialization stays behind dry_run.
    exchange = ExchangeResolver.load_exchange(config, validate=False, load_leverage_tiers=False)
    try:
        require(exchange._config['dry_run'] is True and exchange.trading_mode.value == 'futures'
                and exchange.margin_mode.value == 'isolated', 'C11_NATIVE_DRY_MODE')
        require(all(client.id == 'binance' and not any(getattr(client, key, None) for key in CREDENTIALS)
                    for client in (exchange._api, exchange._api_async)), 'C11_NATIVE_ORDER_CREDENTIALS')
        require(exchange._ws_async is None and not exchange._dry_run_open_orders, 'C11_NATIVE_PAPER_STATE')
        return dict(freqtrade=FREQTRADE, ccxt=CCXT, dry_run=True, exchange='binance',
                    sync_and_async_no_order_credentials=True, markets_loaded=False,
                    orders_submitted=0, bot_started=False)
    finally:
        exchange.close()


def private_mount():
    lines = Path('/proc/self/mountinfo').read_text().splitlines()
    matching = [line.split() for line in lines if line.split()[4] == str(ROOT)]
    require(len(matching) == 1, 'C11_DEDICATED_STATE_MOUNT_REQUIRED')
    require(all(row.split()[4] == str(ROOT) or not row.split()[4].startswith(str(ROOT) + '/')
                for row in lines), 'C11_NESTED_STATE_MOUNT_REFUSED')
    options = set(matching[0][5].split(','))
    require({'rw', 'nodev', 'nosuid', 'noexec'} <= options, 'C11_STATE_MOUNT_OPTIONS')
    require(all(not line.split()[4].startswith(('/producer', '/shared', '/account', '/migration',
                                              '/Volumes/DevilProjects/Workspace/')) for line in lines),
            'C11_FIVE_ACCOUNT_MOUNT_REFUSED')
    info = ROOT.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
            and stat.S_IMODE(info.st_mode) == 0o700, 'C11_PRIVATE_ROOT_IDENTITY')


def verify_state_file(path):
    if not os.path.lexists(path):
        return
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and info.st_nlink == 1
            and stat.S_IMODE(info.st_mode) == 0o600, 'C11_PRIVATE_FILE_IDENTITY')


class PaperState:
    """Held across validation and a future approved runtime lifetime."""
    def __enter__(self):
        private_mount()
        os.umask(0o077)
        self.fd = os.open(LOCK, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.check_locked()
            for db in (TRADE_DB, AUDIT_DB):
                for suffix in ('', '-wal', '-shm', '-journal'):
                    verify_state_file(Path(str(db) + suffix))
        except BaseException:
            os.close(self.fd)
            raise
        return self

    def check_locked(self):
        verify_state_file(LOCK)
        require(os.path.samestat(os.fstat(self.fd), LOCK.lstat()), 'C11_STATE_LOCK_REPLACED')

    def __exit__(self, *exc):
        os.close(self.fd)

    def record(self, phase, receipt_sha256):
        self.check_locked()
        with sqlite3.connect(AUDIT_DB) as db:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('CREATE TABLE IF NOT EXISTS startup (phase TEXT NOT NULL, receipt_sha256 TEXT NOT NULL, '
                       'PRIMARY KEY(phase, receipt_sha256))')
            db.execute('INSERT OR IGNORE INTO startup VALUES (?, ?)', (phase, receipt_sha256))
        verify_state_file(AUDIT_DB)
