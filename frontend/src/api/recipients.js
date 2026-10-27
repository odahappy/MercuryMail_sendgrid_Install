import { requestApi, requestUploadApi } from './index.js';

function appendOptionalName(formData, name) {
  const trimmedName = String(name || '').trim();
  if (trimmedName) {
    formData.append('name', trimmedName);
  }
}

export function resolveRecipientUploadFile(rawFile) {
  const candidate = rawFile?.originFileObj || rawFile;
  if (candidate instanceof Blob) {
    return candidate;
  }
  return null;
}

export async function upload(name, file, onProgress, config = {}) {
  const uploadFile = resolveRecipientUploadFile(file);
  if (!uploadFile) {
    throw new Error('请上传文件或输入邮箱');
  }
  const formData = new FormData();
  appendOptionalName(formData, name);
  const uploadName = String(file?.name || uploadFile.name || 'recipients.txt');
  formData.append('file', uploadFile, uploadName);
  try {
    const response = await requestUploadApi({
      method: 'post',
      url: '/recipients/upload',
      data: formData,
      timeout: 300000,
      onUploadProgress: (event) => {
        if (onProgress && event.total) {
          onProgress(Math.round((event.loaded * 100) / event.total));
        }
      },
      ...config,
    });
    return response.data;
  } catch (error) {
    throw new Error(error?.detail || error?.message || '上传失败，请稍后重试');
  }
}

export async function paste(name, content, config = {}) {
  const payload = { content };
  const trimmedName = String(name || '').trim();
  if (trimmedName) {
    payload.name = trimmedName;
  }
  const response = await requestApi({ method: 'post', url: '/recipients/paste', data: payload, timeout: 120000, ...config });
  return response.data;
}

export async function getLists(config = {}) {
  const response = await requestApi({ method: 'get', url: '/recipients/lists', ...config });
  return response.data;
}

export async function deleteList(listId, config = {}) {
  const response = await requestApi({ method: 'delete', url: `/recipients/lists/${listId}`, ...config });
  return response.data;
}

export async function getDeleteProgress(taskId, config = {}) {
  const response = await requestApi({ method: 'get', url: `/recipients/delete-progress/${taskId}`, ...config });
  return response.data;
}

export async function getRecipients(listId, page = 1, pageSize = 20, config = {}) {
  const response = await requestApi({
    method: 'get',
    url: `/recipients/lists/${listId}/recipients`,
    params: { page, page_size: pageSize },
    ...config,
  });
  return response.data;
}

export async function getProgress(listId, config = {}) {
  const response = await requestApi({ method: 'get', url: `/recipients/progress/${listId}`, ...config });
  return response.data;
}

export async function getImportProgress(taskId, config = {}) {
  const response = await requestApi({ method: 'get', url: `/recipients/import/progress/${taskId}`, ...config });
  return response.data;
}

export async function addRecipients(listId, emails, config = {}) {
  const response = await requestApi({ method: 'post', url: `/recipients/lists/${listId}/recipients`, data: { emails }, ...config });
  return response.data;
}

export async function deleteRecipient(recipientId, config = {}) {
  const response = await requestApi({ method: 'delete', url: `/recipients/${recipientId}`, ...config });
  return response.data;
}
