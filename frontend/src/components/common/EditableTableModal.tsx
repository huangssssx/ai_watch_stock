/**
 * 通用可编辑表格模态框组件
 * 用于统一处理各种CRUD操作，减少代码重复
 */

import React, { useState, useCallback, useEffect } from 'react';
import { Modal, Form, message, Space, Button, Popconfirm } from 'antd';
import { EditOutlined, DeleteOutlined } from '@ant-design/icons';
import type { FormInstance } from 'antd';
import type { ColumnsType } from 'antd/es/table';

// 通用数据类型
export interface EditableItem {
  id: number;
  name: string;
  is_pinned?: boolean;
  [key: string]: unknown;
}

// Props接口
export interface UseEditableTableModalProps<T extends EditableItem> {
  // 数据获取函数
  fetchData: () => Promise<T[]>;
  // 创建函数
  createItem: (item: Partial<T>) => Promise<T>;
  // 更新函数
  updateItem: (id: number, item: Partial<T>) => Promise<T>;
  // 删除函数
  deleteItem: (id: number) => Promise<{ ok: boolean }>;
  // 测试函数（可选）
  testItem?: (id: number) => Promise<unknown>;
  // 表单渲染函数
  renderForm: (form: FormInstance, editingItem: T | null) => React.ReactNode;
  // 表格列定义（除了操作列）
  getColumns: (actions: Actions<T>) => ColumnsType<T>;
  // 项目类型名称
  itemTypeName: string;
  // 是否支持测试
  supportsTest?: boolean;
}

// 操作按钮
export interface Actions<T> {
  edit: (item: T) => void;
  delete: (id: number) => void;
  test?: (item: T) => void;
  togglePin?: (item: T) => void;
}

/**
 * Hook: 返回Modal和表格相关的状态和方法
 */
export function useEditableTableModal<T extends EditableItem>(
  props: UseEditableTableModalProps<T>
) {
  const {
    fetchData,
    createItem,
    updateItem,
    deleteItem,
    testItem,
    getColumns,
    itemTypeName,
    supportsTest = false,
  } = props;

  const [items, setItems] = useState<T[]>([]);
  const [loading, setLoading] = useState(false);
  const [modalVisible, setModalVisible] = useState(false);
  const [editingItem, setEditingItem] = useState<T | null>(null);
  const [form] = Form.useForm();

  // 刷新数据
  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchData();
      // 排序：置顶的在前面
      data.sort((a, b) => {
        if (a.is_pinned === b.is_pinned) return 0;
        return a.is_pinned ? -1 : 1;
      });
      setItems(data);
    } catch {
      message.error(`加载${itemTypeName}列表失败`);
    } finally {
      setLoading(false);
    }
  }, [fetchData, itemTypeName]);

  // 打开新建对话框
  const handleAdd = useCallback(() => {
    setEditingItem(null);
    form.resetFields();
    setModalVisible(true);
  }, [form]);

  // 打开编辑对话框
  const handleEdit = useCallback((item: T) => {
    setEditingItem(item);
    form.setFieldsValue(item);
    setModalVisible(true);
  }, [form]);

  // 处理表单提交
  const handleSubmit = useCallback(async () => {
    try {
      const values = await form.validateFields();
      if (editingItem) {
        await updateItem(editingItem.id, values);
        message.success(`${itemTypeName}已更新`);
      } else {
        await createItem(values);
        message.success(`${itemTypeName}已添加`);
      }
      setModalVisible(false);
      form.resetFields();
      setEditingItem(null);
      refresh();
    } catch {
      message.error(editingItem ? `更新${itemTypeName}失败` : `添加${itemTypeName}失败`);
    }
  }, [form, editingItem, updateItem, createItem, itemTypeName, refresh]);

  // 处理删除
  const handleDelete = useCallback(async (id: number) => {
    try {
      await deleteItem(id);
      message.success(`${itemTypeName}已删除`);
      refresh();
    } catch {
      message.error(`删除${itemTypeName}失败`);
    }
  }, [deleteItem, itemTypeName, refresh]);

  // 处理置顶切换
  const handleTogglePin = useCallback(async (item: T) => {
    try {
      await updateItem(item.id, { is_pinned: !item.is_pinned } as Partial<T>);
      message.success(item.is_pinned ? '已取消置顶' : '已置顶');
      refresh();
    } catch {
      message.error('操作失败');
    }
  }, [updateItem, refresh]);

  // 处理测试
  const handleTest = useCallback(async (item: T) => {
    if (!testItem) return;
    try {
      await testItem(item.id);
      message.success('测试完成');
    } catch {
      message.error('测试失败');
    }
  }, [testItem]);

  // 构建操作列
  const actions: Actions<T> = {
    edit: handleEdit,
    delete: handleDelete,
    togglePin: handleTogglePin,
    ...(supportsTest && testItem && { test: handleTest }),
  };

  const columns = getColumns(actions);

  useEffect(() => {
    refresh();
  }, [refresh]);

  return {
    // 状态
    items,
    loading,
    modalVisible,
    editingItem,
    form,
    columns,
    // 方法
    refresh,
    handleAdd,
    handleEdit,
    handleDelete,
    handleTogglePin,
    handleSubmit,
    setModalVisible,
  };
}

/**
 * 通用操作列渲染器
 */
export function renderActionColumn<T extends EditableItem>(
  actions: Actions<T>,
  options: {
    supportsTest?: boolean;
    supportsPin?: boolean;
  } = {}
) {
  return {
    title: '操作',
    key: 'action',
    width: options.supportsTest ? 200 : 160,
    render: (_: unknown, record: T) => (
      <Space>
        {options.supportsPin && (
          <Button
            type="text"
            icon={record.is_pinned ? '📌' : '📍'}
            onClick={() => actions.togglePin?.(record)}
          />
        )}
        {actions.test && options.supportsTest && (
          <Button
            size="small"
            onClick={() => actions.test!(record)}
          >
            测试
          </Button>
        )}
        <Button
          type="link"
          icon={<EditOutlined />}
          onClick={() => actions.edit(record)}
        />
        <Popconfirm
          title={`确定要删除吗？`}
          onConfirm={() => actions.delete(record.id)}
          okText="确定"
          cancelText="取消"
        >
          <Button type="link" danger icon={<DeleteOutlined />} />
        </Popconfirm>
      </Space>
    ),
  };
}

/**
 * 通用模态框组件
 */
export interface EditableTableModalProps<T extends EditableItem> {
  visible: boolean;
  onCancel: () => void;
  onOk: () => void;
  form: FormInstance;
  title: string;
  editingItem: T | null;
  itemTypeName: string;
  renderForm: (form: FormInstance, editingItem: T | null) => React.ReactNode;
  loading?: boolean;
}

export function EditableTableModal<T extends EditableItem>({
  visible,
  onCancel,
  onOk,
  form,
  title,
  editingItem,
  itemTypeName,
  renderForm,
  loading = false,
}: EditableTableModalProps<T>) {
  return (
    <Modal
      title={title || (editingItem ? `编辑${itemTypeName}` : `添加${itemTypeName}`)}
      open={visible}
      onOk={onOk}
      onCancel={onCancel}
      width={800}
      destroyOnClose
      confirmLoading={loading}
    >
      <Form form={form} layout="vertical">
        {renderForm(form, editingItem)}
      </Form>
    </Modal>
  );
}

