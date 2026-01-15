import React, { useEffect, useState, useMemo } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { Card, Tabs, Spin, Empty, Button, Radio } from 'antd';
import { ArrowLeftOutlined, ArrowUpOutlined, ArrowDownOutlined } from '@ant-design/icons';
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';
import { getStockDaily, getStockHistory } from '../api';

const StockDetail: React.FC = () => {
  const { symbol } = useParams<{ symbol: string }>();
  const navigate = useNavigate();
  const [period, setPeriod] = useState<string>('intraday'); // intraday, daily, weekly, monthly
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  const fetchData = async () => {
    if (!symbol) return;
    setLoading(true);
    try {
      let res;
      if (period === 'intraday') {
        res = await getStockDaily(symbol);
      } else {
        res = await getStockHistory(symbol, period);
      }
      
      if (res.data.ok && res.data.data) {
        setData(res.data.data);
      } else {
        setData([]);
      }
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    let interval: number | undefined;
    
    if (period === 'intraday') {
      // Auto refresh for intraday
      interval = window.setInterval(fetchData, 60000);
    }

    return () => {
      if (interval) window.clearInterval(interval);
    };
  }, [symbol, period]);

  const chartData = useMemo(() => {
    return data;
  }, [data]);

  const lastPrice = data.length ? data[data.length - 1].close : 0;
  const firstPrice = data.length ? (period === 'intraday' ? data[0].open : data[data.length - 2]?.close || data[0].open) : 0;
  // For K-line, change is vs previous close. For intraday, vs open (or pre-close if available, but here open is approx)
  
  const change = lastPrice - firstPrice;
  const percent = firstPrice ? (change / firstPrice) * 100 : 0;
  const color = change >= 0 ? '#cf1322' : '#3f8600';

  return (
    <div>
      <div style={{ marginBottom: 16, display: 'flex', alignItems: 'center' }}>
        <Button icon={<ArrowLeftOutlined />} onClick={() => navigate(-1)} style={{ marginRight: 16 }} />
        <h2 style={{ margin: 0 }}>
            {symbol} 
            {data.length > 0 && (
                <span style={{ marginLeft: 16, color, fontSize: 20 }}>
                    {lastPrice.toFixed(2)}
                    <span style={{ fontSize: 14, marginLeft: 8 }}>
                        {change >= 0 ? <ArrowUpOutlined /> : <ArrowDownOutlined />}
                        {Math.abs(percent).toFixed(2)}%
                    </span>
                </span>
            )}
        </h2>
      </div>

      <Card>
        <Tabs 
            activeKey={period} 
            onChange={setPeriod}
            items={[
                { label: '分时', key: 'intraday' },
                { label: '日线', key: 'daily' },
                { label: '周线', key: 'weekly' },
                { label: '月线', key: 'monthly' },
            ]}
        />
        
        <div style={{ height: 500, marginTop: 16 }}>
            {loading && !data.length ? (
                <div style={{ height: '100%', display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
                    <Spin size="large" />
                </div>
            ) : !data.length ? (
                <Empty description="暂无数据" />
            ) : (
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={chartData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                    <defs>
                      <linearGradient id="colorClose" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor={color} stopOpacity={0.1}/>
                        <stop offset="95%" stopColor={color} stopOpacity={0}/>
                      </linearGradient>
                    </defs>
                    <XAxis 
                        dataKey={period === 'intraday' ? 'time' : 'date'} 
                        minTickGap={50} 
                        tick={{ fontSize: 12 }}
                    />
                    <YAxis domain={['auto', 'auto']} />
                    <CartesianGrid strokeDasharray="3 3" vertical={false} />
                    <Tooltip 
                        labelFormatter={(label) => label}
                        formatter={(value: any) => [Number(value).toFixed(2), '价格']}
                    />
                    <Area 
                        type="monotone" 
                        dataKey="close" 
                        stroke={color} 
                        fillOpacity={1} 
                        fill="url(#colorClose)" 
                        isAnimationActive={false}
                    />
                  </AreaChart>
                </ResponsiveContainer>
            )}
        </div>
      </Card>
    </div>
  );
};

export default StockDetail;
