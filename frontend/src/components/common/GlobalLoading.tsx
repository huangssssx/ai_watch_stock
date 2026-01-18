/**
 * 全局Loading组件
 * 用于显示全局加载状态
 */

import React from 'react';
import { Spin } from 'antd';
import { useLoading } from '../../contexts/LoadingContext';

export interface GlobalLoadingProps {
  /**
   * 自定义加载提示内容
   */
  tip?: string;
  /**
   * 加载指示器大小
   */
  size?: 'small' | 'default' | 'large';
  /**
   * 是否延迟显示加载状态（避免闪烁）
   */
  delay?: number;
}

/**
 * 全局Loading遮罩层组件
 * 当context中globalLoading为true时显示
 */
export function GlobalLoading({ tip, size = 'large', delay = 300 }: GlobalLoadingProps) {
  const { globalLoading, globalMessage } = useLoading();
  const [visible, setVisible] = React.useState(false);

  React.useEffect(() => {
    if (!globalLoading) {
      setVisible(false);
      return;
    }

    const timer = window.setTimeout(() => setVisible(true), delay);
    return () => window.clearTimeout(timer);
  }, [globalLoading, delay]);

  if (!visible) return null;

  return (
    <div
      style={{
        position: 'fixed',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        backgroundColor: 'rgba(255, 255, 255, 0.7)',
        zIndex: 9999,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        flexDirection: 'column',
        gap: 16,
      }}
    >
      <Spin size={size} tip={tip || globalMessage || '加载中...'} />
    </div>
  );
}

/**
 * 内容区域的Loading包装器
 */
export interface LoadingWrapperProps {
  loading: boolean;
  children: React.ReactNode;
  tip?: string;
}

export function LoadingWrapper({ loading, children, tip }: LoadingWrapperProps) {
  if (!loading) {
    return <>{children}</>;
  }

  return (
    <div style={{ textAlign: 'center', padding: '40px 0' }}>
      <Spin tip={tip || '加载中...'} />
    </div>
  );
}

/**
 * 按钮Loading包装器
 * 自动处理异步操作的加载状态
 */
export interface AsyncButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  /**
   * 异步操作函数
   */
  onClickAsync: () => Promise<unknown>;
  /**
   * 子元素
   */
  children: React.ReactNode;
  /**
   * 加载中的按钮文本
   */
  loadingText?: string;
  /**
   * 是否使用全局loading
   */
  globalLoading?: boolean;
}

export function AsyncButton({
  onClickAsync,
  children,
  loadingText,
  globalLoading = false,
  ...rest
}: AsyncButtonProps) {
  const { runAsync } = useLoading();
  const [localLoading, setLocalLoading] = React.useState(false);

  const handleClick = async () => {
    if (globalLoading) {
      await runAsync(onClickAsync, { global: true });
    } else {
      setLocalLoading(true);
      try {
        await onClickAsync();
      } finally {
        setLocalLoading(false);
      }
    }
  };

  const isLoading = globalLoading ? false : localLoading;
  const displayText = isLoading && loadingText ? loadingText : children;

  return (
    <button {...rest} onClick={handleClick} disabled={isLoading || rest.disabled}>
      {isLoading ? <Spin size="small" /> : displayText}
    </button>
  );
}

/**
 * Loading指示器组件（非遮罩）
 */
export interface LoadingSpinnerProps {
  /**
   * 是否显示
   */
  spinning?: boolean;
  /**
   * 提示文本
   */
  tip?: string;
  /**
   * 大小
   */
  size?: 'small' | 'default' | 'large';
}

export function LoadingSpinner({ spinning = true, tip, size = 'default' }: LoadingSpinnerProps) {
  if (!spinning) return null;

  return <Spin size={size} tip={tip} />;
}
