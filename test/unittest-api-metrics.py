#!/usr/bin/env python

import asyncio
import os
import sys
import unittest

sys.path.append('../api')

os.environ.setdefault('ANARCHY_COMPONENT', 'api')

from metrics import (
    AppMetrics,
    MetricsService,
    TimerDecoratorMeta,
    async_timer,
    sync_timer,
)


class TestAppMetrics(unittest.TestCase):
    def test_process_time_histogram_registered(self):
        self.assertEqual(AppMetrics.process_time.name, "anarchy_process_time_seconds")

    def test_process_time_in_registry(self):
        collectors = {c.name for c in AppMetrics.registry.get_all()}
        self.assertIn("anarchy_process_time_seconds", collectors)

    def test_http_request_duration_histogram_registered(self):
        self.assertEqual(AppMetrics.http_request_duration.name, "anarchy_api_http_request_duration_seconds")

    def test_http_request_duration_in_registry(self):
        collectors = {c.name for c in AppMetrics.registry.get_all()}
        self.assertIn("anarchy_api_http_request_duration_seconds", collectors)


class TestSyncTimer(unittest.TestCase):
    def test_success(self):
        @sync_timer(app="TestSync")
        def add(a, b):
            return a + b

        result = add(2, 3)
        self.assertEqual(result, 5)

    def test_error(self):
        @sync_timer(app="TestSync")
        def fail():
            raise ValueError("boom")

        with self.assertRaises(ValueError):
            fail()

    def test_preserves_function_name(self):
        @sync_timer(app="TestSync")
        def my_original_name():
            pass

        self.assertEqual(my_original_name.__name__, "my_original_name")


class TestAsyncTimer(unittest.TestCase):
    def test_success(self):
        @async_timer(app="TestAsync")
        async def async_add(a, b):
            return a + b

        result = asyncio.run(async_add(10, 20))
        self.assertEqual(result, 30)

    def test_error(self):
        @async_timer(app="TestAsync")
        async def async_fail():
            raise RuntimeError("async boom")

        with self.assertRaises(RuntimeError):
            asyncio.run(async_fail())

    def test_preserves_function_name(self):
        @async_timer(app="TestAsync")
        async def my_async_func():
            pass

        self.assertEqual(my_async_func.__name__, "my_async_func")


class TestTimerDecoratorMeta(unittest.TestCase):
    def setUp(self):
        class SampleClass(metaclass=TimerDecoratorMeta):
            def instance_method(self):
                return "instance"

            @classmethod
            def class_method(cls):
                return "classmethod"

            def __str__(self):
                return "SampleClass"

            def __repr__(self):
                return "<SampleClass>"

        self.cls = SampleClass
        self.obj = SampleClass()

    def test_instance_method_works(self):
        self.assertEqual(self.obj.instance_method(), "instance")

    def test_classmethod_works(self):
        self.assertEqual(self.cls.class_method(), "classmethod")

    def test_dunder_not_wrapped(self):
        self.assertEqual(str(self.obj), "SampleClass")
        self.assertEqual(repr(self.obj), "<SampleClass>")

    def test_metaclass_inherited(self):
        class Parent(metaclass=TimerDecoratorMeta):
            def parent_method(self):
                return "parent"

        class Child(Parent):
            def child_method(self):
                return "child"

        child = Child()
        self.assertEqual(child.parent_method(), "parent")
        self.assertEqual(child.child_method(), "child")


class TestMetricsService(unittest.TestCase):
    def test_service_has_registry(self):
        self.assertIs(MetricsService.service.registry, AppMetrics.registry)

    def test_start_is_coroutine(self):
        self.assertTrue(asyncio.iscoroutinefunction(MetricsService.start))

    def test_stop_is_coroutine(self):
        self.assertTrue(asyncio.iscoroutinefunction(MetricsService.stop))


if __name__ == '__main__':
    unittest.main()
