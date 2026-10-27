import { useCallback, useEffect, useRef } from 'react';

export function useAbortController() {
  const controllersRef = useRef(new Set());

  const createAbortController = useCallback(() => {
    const controller = new AbortController();
    controllersRef.current.add(controller);
    controller.signal.addEventListener('abort', () => {
      controllersRef.current.delete(controller);
    }, { once: true });
    return controller;
  }, []);

  const releaseAbortController = useCallback((controller) => {
    if (controller) {
      controllersRef.current.delete(controller);
    }
  }, []);

  const cleanupAllControllers = useCallback(() => {
    const pendingCount = controllersRef.current.size;
    if (pendingCount > 0 && import.meta.env.DEV) {
      console.debug(`[MercuryMail] 组件卸载，取消 ${pendingCount} 个未完成请求`);
    }
    for (const controller of controllersRef.current) {
      try {
        controller.abort();
      } catch {
        // ignore
      }
    }
    controllersRef.current.clear();
  }, []);

  useEffect(() => cleanupAllControllers, [cleanupAllControllers]);

  return { createAbortController, releaseAbortController, cleanupAllControllers };
}
