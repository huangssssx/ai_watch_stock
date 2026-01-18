import { Modal, Spin } from 'antd';
import type { SpinProps } from 'antd';
import { useLoading } from '../../contexts/LoadingContext';

export interface GlobalLoadingProps {
  tip?: string;
  size?: SpinProps['size'];
  delay?: number;
}

export function GlobalLoading({ tip, size = 'large', delay = 300 }: GlobalLoadingProps) {
  const { globalLoading, globalMessage } = useLoading();

  if (!globalLoading) return null;

  return (
    <Modal
      open
      closable={false}
      footer={null}
      centered
      maskClosable={false}
      width={360}
      styles={{ body: { display: 'flex', justifyContent: 'center', alignItems: 'center', padding: 24 } }}
    >
      <Spin size={size} delay={delay} tip={tip ?? globalMessage ?? '加载中...'} />
    </Modal>
  );
}
