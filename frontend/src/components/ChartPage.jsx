import React, { useEffect, useMemo, useState } from 'react';
import EChart from './EChart';
import { useParams, useSearchParams, useNavigate } from 'react-router-dom';
import { Card, Select, Modal, Form, Input, message, Button } from 'antd';
import { getStrategy, getAlerts, getMinuteData, updateAlert } from '../api';

const ChartPage = () => {
  const { id } = useParams();
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const [strategy, setStrategy] = useState(null);
  const [alerts, setAlerts] = useState([]);
  const [kdata, setKdata] = useState([]);
  const [period, setPeriod] = useState('1');
  const [editingAlert, setEditingAlert] = useState(null);
  const [form] = Form.useForm();

  useEffect(() => {
    loadStrategy();
  }, [id]);

  useEffect(() => {
    if (strategy?.symbol) {
      loadKline(strategy.symbol, period);
      loadAlerts();
    }
  }, [strategy, period]);

  const loadStrategy = async () => {
    try {
      const res = await getStrategy(id);
      setStrategy(res.data);
    } catch (e) {
      message.error('加载策略失败');
    }
  };

  const loadAlerts = async () => {
    try {
      const res = await getAlerts(id);
      setAlerts(res.data || []);
      const focusId = searchParams.get('alertId');
      if (focusId) {
        const target = (res.data || []).find(a => String(a.id) === String(focusId));
        if (target) setEditingAlert(target);
      }
    } catch (e) {
      // ignore
    }
  };

  const loadKline = async (symbol, p) => {
    try {
      const today = new Date();
      const ds = `${today.getFullYear()}${String(today.getMonth() + 1).padStart(2, '0')}${String(today.getDate()).padStart(2, '0')}`;
      const res = await getMinuteData(symbol, p, { date: ds, today_only: true });
      const rows = res.data?.data || res.data || [];
      setKdata(rows);
    } catch (e) {
      message.error('加载当日K线失败');
    }
  };

  const candleSeries = useMemo(() => {
    // Normalize columns: Chinese or English
    const tKey = ['时间', 'time', 'datetime'].find(k => k in (kdata[0] || {})) || '时间';
    const oKey = ['开盘', 'open'].find(k => k in (kdata[0] || {})) || '开盘';
    const hKey = ['最高', 'high'].find(k => k in (kdata[0] || {})) || '最高';
    const lKey = ['最低', 'low'].find(k => k in (kdata[0] || {})) || '最低';
    const cKey = ['收盘', 'close'].find(k => k in (kdata[0] || {})) || '收盘';
    const x = kdata.map(r => r[tKey]);
    const y = kdata.map(r => [r[oKey], r[cKey], r[lKey], r[hKey]]);
    return { x, y, tKey, cKey };
  }, [kdata]);

  const alertPoints = useMemo(() => {
    if (!alerts?.length || !candleSeries?.x?.length) return [];
    const xs = candleSeries.x;
    const cKey = candleSeries.cKey;
    return alerts.map(a => {
      const ts = a.timestamp ? new Date(a.timestamp) : null;
      let idx = -1;
      if (ts) {
        const tsStr = ts.toLocaleString('zh-CN', { hour12: false }).replace(/\//g, '-');
        // try match by hour:minute substring
        idx = xs.findIndex(s => String(s).includes(ts.toTimeString().slice(0,5)));
      }
      if (idx < 0) idx = xs.length - 1;
      const price = (kdata[idx] || {})[cKey];
      return {
        name: `Alert#${a.id}`,
        value: [xs[idx], price],
        msg: a.message,
        alertId: a.id,
        level: a.level,
      };
    });
  }, [alerts, candleSeries, kdata]);

  const option = useMemo(() => {
    return {
      animation: false,
      tooltip: { trigger: 'axis' },
      xAxis: { type: 'category', data: candleSeries.x, boundaryGap: true },
      yAxis: { scale: true },
      series: [
        {
          type: 'candlestick',
          data: candleSeries.y,
        },
        {
          type: 'scatter',
          data: alertPoints.map(p => p.value),
          symbol: 'pin',
          symbolSize: 20,
          tooltip: {
            formatter: (params) => {
              const p = alertPoints[params.dataIndex];
              return `${p.name}<br/>${p.msg}`;
            }
          },
        }
      ]
    };
  }, [candleSeries, alertPoints]);

  const onEvents = {
    click: (params) => {
      if (params.seriesType === 'scatter') {
        const idx = params.dataIndex;
        const p = alertPoints[idx];
        if (p) {
          setEditingAlert({
            id: p.alertId,
            message: p.msg,
            level: p.level,
          });
          form.setFieldsValue({ message: p.msg, level: p.level });
        }
      }
    }
  };

  const handleUpdate = async () => {
    try {
      const values = await form.validateFields();
      await updateAlert(editingAlert.id, values);
      message.success('告警已更新');
      setEditingAlert(null);
      loadAlerts();
    } catch (e) {
      message.error('更新失败');
    }
  };

  return (
    <Card
      title={strategy ? `${strategy.name} - ${strategy.symbol} 当日K线` : '加载中'}
      extra={
        <div style={{ display: 'flex', gap: 8 }}>
          <Select
            value={period}
            onChange={setPeriod}
            options={[
              { value: '1', label: '1分钟' },
              { value: '5', label: '5分钟' },
              { value: '15', label: '15分钟' },
              { value: '30', label: '30分钟' },
              { value: '60', label: '60分钟' },
            ]}
            style={{ width: 120 }}
          />
          <Button onClick={() => navigate(-1)}>返回</Button>
        </div>
      }
    >
      <EChart option={option} style={{ height: 520 }} onEvents={onEvents} />

      <Modal
        title={`编辑告警 #${editingAlert?.id || ''}`}
        open={!!editingAlert}
        onCancel={() => setEditingAlert(null)}
        onOk={handleUpdate}
        okText="保存"
        cancelText="取消"
      >
        <Form form={form} layout="vertical">
          <Form.Item name="message" label="提示文本" rules={[{ required: true }]}>
            <Input.TextArea rows={4} />
          </Form.Item>
          <Form.Item name="level" label="级别" rules={[{ required: true }]}>
            <Select
              options={[
                { value: 'INFO', label: 'INFO' },
                { value: 'WARNING', label: 'WARNING' },
                { value: 'ERROR', label: 'ERROR' },
              ]}
            />
          </Form.Item>
        </Form>
      </Modal>
    </Card>
  );
};

export default ChartPage;
