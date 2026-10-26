"""Restore prompts require explicit approval and preserve unattended behavior."""
import unittest
from unittest import mock

import backup


class InteractiveRestoreTests(unittest.TestCase):
    def test_blank_or_invalid_input_keeps_waiting_for_explicit_answer(self):
        for answer, expected in [('y', True), (' YES ', True), ('n', False), ('No', False)]:
            with self.subTest(answer=answer), mock.patch('builtins.input', side_effect=['', ' ', 'invalid', answer]) as read, \
                    mock.patch('builtins.print'):
                self.assertIs(backup.confirm_restore('确认？'), expected)
                self.assertEqual(read.call_count, 4)

    def test_end_of_input_cancels_instead_of_confirming(self):
        with mock.patch('builtins.input', side_effect=EOFError), mock.patch('builtins.print'):
            with self.assertRaisesRegex(backup.BackupError, '取消'):
                backup.confirm_restore('确认？')

    def setUp(self):
        self.args = backup.parser().parse_args(['restore', 'run', 'archive.tar.gz'])
        self.args.components = ['mariadb', 'opensearch', 'kafka']
        self.manifest = {'kafka': {'topics': [{'name': 'openbkn.audit.v1', 'partitions': [{'high': 117}]}]}}
        for stream in (backup.sys.stdin, backup.sys.stderr):
            patch = mock.patch.object(stream, 'isatty', return_value=True)
            patch.start()
            self.addCleanup(patch.stop)

    def test_missing_options_are_granted_only_by_explicit_answers(self):
        with mock.patch.object(backup, 'confirm_restore', side_effect=[True, True, True]) as ask:
            backup.prepare_restore_options(self.args, self.manifest)
        self.assertEqual(ask.call_count, 3)
        self.assertTrue(self.args.kafka_ledger_restored)
        self.assertTrue(self.args.overwrite)
        self.assertTrue(self.args.replace_repository)

    def test_stop_waits_for_command_success_before_marking_writers_stopped(self):
        with mock.patch.object(backup, 'confirm_restore', return_value=True), \
                mock.patch.object(backup, 'run') as run:
            backup.prepare_restore_stop(self.args)
        self.assertTrue(self.args.writers_stopped)
        command = run.call_args.args[0]
        self.assertEqual(command[-3:], ['stop', '--namespace', 'openbkn'])
        with mock.patch.object(backup, 'confirm_restore', return_value=True), \
                mock.patch.object(backup, 'run', side_effect=backup.BackupError('stop failed')):
            self.args.writers_stopped = False
            with self.assertRaisesRegex(backup.BackupError, 'stop failed'):
                backup.prepare_restore_stop(self.args)
        self.assertFalse(self.args.writers_stopped)

    def test_refusing_stop_never_changes_workloads(self):
        with mock.patch.object(backup, 'confirm_restore', return_value=False), \
                mock.patch.object(backup, 'run') as run:
            with self.assertRaisesRegex(backup.BackupError, '取消'):
                backup.prepare_restore_stop(self.args)
        run.assert_not_called()
        self.assertFalse(self.args.writers_stopped)

    def test_refusing_ledger_confirmation_cancels_before_stopping_services(self):
        with mock.patch.object(backup, 'confirm_restore', return_value=False), \
                mock.patch.object(backup, 'run') as run:
            with self.assertRaisesRegex(backup.BackupError, '未确认 Kafka'):
                backup.prepare_restore_options(self.args, self.manifest)
        run.assert_not_called()

    def test_noninteractive_check_and_non_tty_never_prompt_or_stop(self):
        for case in ('flag', 'check', 'pipe'):
            with self.subTest(case=case):
                self.args.non_interactive = case == 'flag'
                self.args.action = 'check' if case == 'check' else 'run'
                with mock.patch.object(backup.sys.stdin, 'isatty', return_value=case != 'pipe'), \
                        mock.patch.object(backup, 'confirm_restore') as ask, \
                        mock.patch.object(backup, 'run') as run:
                    backup.prepare_restore_options(self.args, self.manifest)
                    backup.prepare_restore_stop(self.args)
                ask.assert_not_called()
                run.assert_not_called()

    def test_all_confirmations_supplied_never_prompt(self):
        self.args.writers_stopped = self.args.overwrite = True
        self.args.replace_repository = self.args.kafka_ledger_restored = True
        with mock.patch.object(backup, 'confirm_restore') as ask, mock.patch.object(backup, 'run') as run:
            backup.prepare_restore_options(self.args, self.manifest)
            backup.prepare_restore_stop(self.args)
        ask.assert_not_called()
        run.assert_not_called()


if __name__ == '__main__':
    unittest.main()
