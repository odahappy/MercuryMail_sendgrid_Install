import { useCallback, useEffect, useRef, useState } from 'react';
import { getErrorMessage } from '../api/index.js';
import { getLists } from '../api/recipients.js';
import { isImportTaskFinished } from '../utils/recipientImportTasks.js';
import { useAbortController } from './useAbortController.js';

export function useRecipientLists({ autoLoad = true, silent = false } = {}) {
  const { createAbortController, cleanupAllControllers } = useAbortController();
  const requestIdRef = useRef(0);
  const isMountedRef = useRef(true);
  const [lists, setLists] = useState([]);
  const [loading, setLoading] = useState(false);

  const loadLists = useCallback(async ({ showError = !silent } = {}) => {
    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;
    const controller = createAbortController();
    setLoading(true);
    try {
      const data = await getLists({ signal: controller.signal });
      if (!isMountedRef.current || requestIdRef.current !== requestId) return [];
      const nextLists = Array.isArray(data) ? data : [];
      setLists(nextLists);
      return nextLists;
    } catch (error) {
      if (error?.code === 'ERR_CANCELED') return [];
      if (!isMountedRef.current || requestIdRef.current !== requestId) return [];
      if (showError) {
        throw new Error(getErrorMessage(error, '加载收件人池失败'));
      }
      return [];
    } finally {
      if (isMountedRef.current && requestIdRef.current === requestId) {
        setLoading(false);
      }
    }
  }, [createAbortController, silent]);

  const refreshLists = useCallback(async () => loadLists({ showError: !silent }), [loadLists, silent]);

  const removeListFromState = useCallback((listId) => {
    setLists((current) => current.filter((item) => item.id !== listId));
  }, []);

  const upsertListToState = useCallback((list) => {
    if (!list?.id) return;
    setLists((current) => {
      const existing = current.find((item) => item.id === list.id);
      const nextList = {
        ...(existing || {}),
        ...list,
        status: isImportTaskFinished(list.status) ? 'ready' : (list.status || existing?.status || 'ready'),
      };
      const next = current.filter((item) => item.id !== list.id);
      next.unshift(nextList);
      return next;
    });
  }, []);

  useEffect(() => {
    isMountedRef.current = true;
    if (autoLoad) {
      loadLists({ showError: !silent }).catch(() => {});
    }
    return () => {
      isMountedRef.current = false;
      cleanupAllControllers();
    };
  }, [autoLoad, cleanupAllControllers, loadLists, silent]);

  return {
    lists,
    loading,
    setLists,
    refreshLists,
    loadLists,
    removeListFromState,
    upsertListToState,
  };
}
