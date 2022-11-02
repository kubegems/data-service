package com.cloudminds.bigdata.dataservice.quoto.manage.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.collections4.SetUtils;

import com.alibaba.fastjson.JSONObject;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.DataCommonResponse;

public class QuotoCaculateUtils {
	/**
	 * 判断传入的字符是不是0-9的数字
	 * 
	 * @param str 传入的字符串
	 * @return
	 */
	public static boolean isOperator(String temp) {
		boolean isOperator = temp.equals("(") || temp.equals(")") || temp.equals("+") || temp.equals("-")
				|| temp.equals("*") || temp.equals("/") || temp.equals("#") || temp.equals("&");
		return isOperator;
	}

	/**
	 * 比较当前操作符与栈顶元素操作符优先级，如果比栈顶元素优先级高，则返回true，否则返回false
	 * 
	 * @param str 需要进行比较的字符
	 * @return 比较结果 true代表比栈顶元素优先级高，false代表比栈顶元素优先级低
	 */
	public static boolean compare(char str, Stack<Character> priStack) {
		if (priStack.empty()) {
			// 当为空时，显然 当前优先级最低，返回高
			return true;
		}
		char last = (char) priStack.lastElement();
		// 如果栈顶为'('显然，优先级最低，')'不可能为栈顶。
		if (last == '(') {
			return true;
		}
		switch (str) {
		case '#':
			return false;// 结束符
		case '(':
			// '('优先级最高,显然返回true
			return true;
		case ')':
			// ')'优先级最低，
			return false;
		case '*': {
			// '*/'优先级只比'+-&'高
			if (last == '+' || last == '-'|| last == '&')
				return true;
			else
				return false;
		}
		case '/': {
			if (last == '+' || last == '-' || last == '&')
				return true;
			else
				return false;
		}
		// '+-'为最低，一直返回false
		case '+':
			return false;
		case '-':
			return false;
		case '&':
			return false;
		}
		return true;
	}

	/**
	 * 
	 * @param aValue           参与计算的第一个数
	 * @param bvalue           参与计算的第二个数
	 * @param op               操作符
	 * @param exceptionMessage 除数为0时的异常信息
	 * @return
	 */
	public static BigDecimal caluclate(BigDecimal aValue, BigDecimal bvalue, String op, String exceptionMessage) {
		if (op.equals("+")) {
			return aValue.add(bvalue);
		} else if (op.equals("-")) {
			return aValue.subtract(bvalue);
		} else if (op.equals("*")) {
			return aValue.multiply(bvalue);
		} else {
			if (bvalue.floatValue() == 0) {
				// 抛异常，除数的分母为0
				throw new UnsupportedOperationException(exceptionMessage);
			}
			return aValue.divide(bvalue, 4, RoundingMode.HALF_UP);
		}
	}

	/**
	 * 两个指标做四则运算
	 * 
	 * @param a  参与计算的第一个数
	 * @param b  参与计算的第二个数
	 * @param op 操作符
	 */
	public static DataCommonResponse CalculateValue(DataCommonResponse a, DataCommonResponse b, String op) {
		int cycle = Math.max(a.getCycle(), b.getCycle());
		a.setCycle(cycle);
		b.setCycle(cycle);
		if (op.equals("&")) {
			if (a.getType() == 0) {
				return b;
			} else if (a.getType() == 3 || b.getType() == 3) {
				throw new UnsupportedOperationException("常数不能做&运算");
			} else if (b.getType() == 0) {
				return a;
			} else {
				if (b.getDimensions() == null && a.getDimensions() == null) {
					JSONObject aObject = JSONObject.parseObject(a.getData().toString());
					JSONObject bObject = JSONObject.parseObject(b.getData().toString());
					if (a.getType() == 1 && b.getType() == 1) {
						for (String field : b.getFields()) {
							aObject.put(field, bObject.get(field));
						}
						a.getFields().addAll(b.getFields());
						a.setData(aObject);
						return a;
					} else {
						throw new UnsupportedOperationException("没有维度的数据异常");
					}
				} else if (SetUtils.isEqualSet(b.getDimensions(), a.getDimensions())) {
					if (a.getType() == 1) {
						JSONObject aObject = JSONObject.parseObject(a.getData().toString());
						if (b.getType() == 1) {
							JSONObject bObject = JSONObject.parseObject(b.getData().toString());
							if (dimensionValueEqual(aObject, bObject, a.getDimensions())) {
								for (String field : b.getFields()) {
									aObject.put(field, bObject.get(field));
								}
								a.getFields().addAll(b.getFields());
								a.setData(aObject);
								return a;
							} else {
								throw new UnsupportedOperationException("&运算,两个指标的数据量需一致,并维度的值也能一一对应");
							}
						} else {
							throw new UnsupportedOperationException("&运算,两个指标的数据量需一致");
						}
					} else {
						if (b.getType() == 2) {
							List<JSONObject> alist = JSONObject.parseArray(a.getData().toString(), JSONObject.class);
							List<JSONObject> bList = JSONObject.parseArray(b.getData().toString(), JSONObject.class);
							for (int i = 0; i < alist.size(); i++) {
								JSONObject aObject = alist.get(i);
								for (int j = 0; j < bList.size(); j++) {
									JSONObject bObject = bList.get(j);
									if (dimensionValueEqual(aObject, bObject, a.getDimensions())) {
										for (String field : b.getFields()) {
											aObject.put(field, bObject.get(field));
										}
										alist.set(i, aObject);
										bList.remove(j);
										break;
									}
									if (j == bList.size() - 1) {
										throw new UnsupportedOperationException("指标维度值不能一一对应时,是不能做&运算");
									}
								}
							}
							a.setData(alist);
							a.getFields().addAll(b.getFields());
							return a;
						} else {
							throw new UnsupportedOperationException("&运算,两个指标的数据量需一致");
						}
					}
				} else {
					throw new UnsupportedOperationException("有维度时，维度不同不能做&运算");
				}
			}
		}

		if((b.getFields()!=null &&b.getFields().size()>1)||(a.getFields()!=null&&a.getFields().size()>1)) {
			throw new UnsupportedOperationException("&只能是最后一步运算,它之后不能做+-*/运算");
		}
		// a为0 代表a查出来没有数据
		if (a.getType() == 0) {
			if (op.equals("+")) {
				return b;
			} else if (op.equals("*")) {
				return a;
			} else if (op.equals("/")) {
				if (b.getType() != 0) {
					return a;
				} else {
					// 抛异常，除数的分母为0
					throw new UnsupportedOperationException("除法里分母为0,具体分母数据：" + b.getFields() + "(" + b.getData() + ")");
				}
			} else { // 减法
				if (b.getType() == 0) {
					return a;
				} else {
					// 将b的数变成相反数
					if (b.getType() == 3) {
						b.setData(new BigDecimal(b.getData().toString()).multiply(new BigDecimal("-1")));
					} else if (b.getType() == 2) {
						List<JSONObject> list = JSONObject.parseArray(b.getData().toString(), JSONObject.class);
						for (int i = 0; i < list.size(); i++) {
							BigDecimal value = list.get(i).getBigDecimal(b.getFields().iterator().next());
							list.get(i).put(b.getFields().iterator().next(), value.multiply(new BigDecimal("-1")));
						}
						b.setData(list);
					} else {
						JSONObject object = JSONObject.parseObject(b.getData().toString());
						object.put(b.getFields().iterator().next(), object.getBigDecimal(b.getFields().iterator().next()).multiply(new BigDecimal("-1")));
						b.setData(object);
					}
					return b;
				}
			}
		}
		// a为3 代表从外面传进来的数
		if (a.getType() == 3) {
			BigDecimal aValue = new BigDecimal(a.getData().toString());
			if (b.getType() == 0) {
				if (op.equals("+") || op.equals("-")) {
					return a;
				} else if (op.equals("*")) {
					return b;
				} else {
					// 抛异常，除数的分母为0
					throw new UnsupportedOperationException("除法里分母为0,具体分母数据：" + b.getFields() + "(" + b.getData() + ")");
				}
			} else if (b.getType() == 3) {
				b.setData(caluclate(aValue, new BigDecimal(b.getData().toString()), op, "除法里分母为0,请检查加工方式"));
				return b;
			} else if (b.getType() == 2) {
				List<JSONObject> list = JSONObject.parseArray(b.getData().toString(), JSONObject.class);
				for (int i = 0; i < list.size(); i++) {
					list.get(i).put(b.getFields().iterator().next(), caluclate(aValue, list.get(i).getBigDecimal(b.getFields().iterator().next()), op,
							"除法里分母为0,具体分母数据：" + b.getFields() + "(" + list.get(i) + ")"));
				}
				b.setData(list);
				return b;
			} else {
				JSONObject object = JSONObject.parseObject(b.getData().toString());
				object.put(b.getFields().iterator().next(), caluclate(aValue, object.getBigDecimal(b.getFields().iterator().next()), op,
						"除法里分母为0,具体分母数据：" + b.getFields() + "(" + object + ")"));
				b.setData(object);
				return b;
			}

		}
		// 代表a查出来的数据是只有一个
		if (a.getType() == 1) {
			JSONObject aObject = JSONObject.parseObject(a.getData().toString());
			BigDecimal aValue = aObject.getBigDecimal(a.getFields().iterator().next());
			if (b.getType() == 0) {
				if (op.equals("+") || op.equals("-")) {
					return a;
				} else if (op.equals("*")) {
					return b;
				} else {
					throw new UnsupportedOperationException("除法里分母为0,具体分母数据：" + b.getFields() + "(" + b.getData() + ")");
				}
			} else if (b.getType() == 3) {
				aObject.put(a.getFields().iterator().next(),
						caluclate(aValue, new BigDecimal(b.getData().toString()), op, "除法里分母为0,请检查加工方式"));
				a.setData(aObject);
				return a;
			} else if (b.getType() == 1) {
				// 都有维度
				if (b.getDimensions() != null && a.getDimensions() != null) {
					// 如果维度不同，报错
					if (!SetUtils.isEqualSet(b.getDimensions(), a.getDimensions())) {
						throw new UnsupportedOperationException("维度不同的指标,不能做四则运算");
					} else {
						// 维度相同的运算 1对1
						JSONObject bObject = JSONObject.parseObject(b.getData().toString());
						BigDecimal bValue = bObject.getBigDecimal(b.getFields().iterator().next());
						if (dimensionValueEqual(aObject, bObject, a.getDimensions())) {
							aObject.put(a.getFields().iterator().next(), caluclate(aValue, bValue, op,
									"除法里分母为0,具体分母数据：" + b.getFields() + "(" + b.getData() + ")"));
							a.setData(aObject);
							return a;
						} else {
							if (op.equals("*") || op.equals("/")) {
								throw new UnsupportedOperationException("维度的值不同的指标,不能做*/运算");
							} else {
								if (op.equals("-")) {
									// 将b的数变成相反数
									bValue = bValue.negate();
									bObject.put(b.getFields().iterator().next(), bValue);
								}
								// 合并成list返回
								List<JSONObject> list = new ArrayList<JSONObject>();
								list.add(aObject);
								list.add(bObject);
								a.setData(list);
								return a;
							}
						}
					}

				} else {
					// 一个有维度，一个没有维度，或者都没维度 进行四则运算
					JSONObject bObject = JSONObject.parseObject(b.getData().toString());
					BigDecimal bValue = bObject.getBigDecimal(b.getFields().iterator().next());
					aObject.put(a.getFields().iterator().next(),
							caluclate(aValue, bValue, op, "除法里分母为0,具体分母数据：" + b.getFields() + "(" + b.getData() + ")"));
					a.setData(aObject);
					if (b.getDimensions() != null && b.getDimensions().size() > 0) {
						a.setDimensions(b.getDimensions());
						a.setDimensionIds(b.getDimensionIds());
					}
					return a;
				}
			} else { // b为多维的
				List<JSONObject> list = JSONObject.parseArray(b.getData().toString(), JSONObject.class);
				if (a.getDimensions() == null || a.getDimensions().size() == 0) {
					for (int i = 0; i < list.size(); i++) {
						list.get(i).put(b.getFields().iterator().next(), caluclate(aValue, list.get(i).getBigDecimal(b.getFields().iterator().next()), op,
								"除法里分母为0,具体分母数据：" + b.getFields() + "(" + list.get(i) + ")"));
					}
					b.setData(list);
					return b;
				} else {
					if (b.getDimensions() == null || b.getDimensions().size() == 0) {
						throw new UnsupportedOperationException("有维度的数据与多条没有维度的数据不能做四则运算");
					} else {
						if (!SetUtils.isEqualSet(b.getDimensions(), a.getDimensions())) {
							throw new UnsupportedOperationException("维度不同的指标,不能做四则运算");
						} else {
							// 维度相同的运算 1对多
							if (op.equals("*") || op.equals("/")) {
								throw new UnsupportedOperationException("维度相同，数目不相同的指标不能做*/运算");
							}
							// 相减，将b变成负数
							if (op.equals("-")) {
								for (int i = 0; i < list.size(); i++) {
									list.get(i).put(b.getFields().iterator().next(), list.get(i).getBigDecimal(b.getFields().iterator().next()).negate());
								}
							}
							return addN1(b, a);
						}
					}
				}
			}
		}

		// 代表查出来的数据是多个
		if (a.getType() == 2) {
			List<JSONObject> list = JSONObject.parseArray(a.getData().toString(), JSONObject.class);
			if (b.getType() == 0) {
				if (op.equals("+") || op.equals("-")) {
					return a;
				} else if (op.equals("*")) {
					return b;
				} else {
					throw new UnsupportedOperationException("除法里分母为0,具体分母数据：" + b.getFields() + "(" + b.getData() + ")");
				}
			} else if (b.getType() == 3) {
				for (int i = 0; i < list.size(); i++) {
					BigDecimal aValue = list.get(i).getBigDecimal(a.getFields().iterator().next());
					list.get(i).put(a.getFields().iterator().next(),
							caluclate(aValue, new BigDecimal(b.getData().toString()), op, "除法里分母为0,请检查加工方式"));
				}
				a.setData(list);
				return a;
			} else if (b.getType() == 1) {
				// b为1个
				JSONObject bObject = JSONObject.parseObject(b.getData().toString());
				BigDecimal bValue = bObject.getBigDecimal(b.getFields().iterator().next());
				if (b.getDimensions() == null || b.getDimensions().size() == 0) {
					for (int i = 0; i < list.size(); i++) {
						list.get(i).put(a.getFields().iterator().next(), caluclate(list.get(i).getBigDecimal(a.getFields().iterator().next()), bValue, op,
								"除法里分母为0,具体分母数据：" + bObject));
					}
					a.setData(list);
					return a;
				} else {
					if (a.getDimensions() == null || a.getDimensions().size() == 0) {
						throw new UnsupportedOperationException("有维度的数据与多条没有维度的数据不能做四则运算");
					} else {
						if (!SetUtils.isEqualSet(b.getDimensions(), a.getDimensions())) {
							throw new UnsupportedOperationException("维度不同的指标,不能做四则运算");
						} else {
							// 维度相同的运算 n对1
							if (op.equals("*") || op.equals("/")) {
								throw new UnsupportedOperationException("维度相同，数目不相同的指标不能做*/运算");
							}
							// 做+-操作 如果是-，将b变为相反数
							if (op.equals("-")) {
								bObject.put(b.getFields().iterator().next(), bValue.negate());
							}
							return addN1(a, b);
						}
					}
				}
			} else {
				// b为多维数据
				if (a.getDimensions() == null || a.getDimensions().size() == 0 || b.getDimensions() == null
						|| b.getDimensions().size() == 0
						|| !SetUtils.isEqualSet(a.getDimensions(), b.getDimensions())) {
					throw new UnsupportedOperationException("两个多条的指标数据维度不同时是不能做四则运算的");
				}
				List<JSONObject> bList = JSONObject.parseArray(b.getData().toString(), JSONObject.class);
				if (op.equals("*") || op.equals("/")) {
					if (list.size() != bList.size()) {
						throw new UnsupportedOperationException("都包含多个数据的指标,数据量不相等是不能做*/运算");
					}
				}
				for (int i = 0; i < list.size(); i++) {
					JSONObject aObject = list.get(i);
					for (int j = 0; j < bList.size(); j++) {
						JSONObject bObject = bList.get(j);
						if (dimensionValueEqual(aObject, bObject, a.getDimensions())) {
							aObject.put(a.getFields().iterator().next(), caluclate(aObject.getBigDecimal(a.getFields().iterator().next()),
									bObject.getBigDecimal(b.getFields().iterator().next()), op, "除法里分母为0,具体分母数据：" + bObject));
							list.set(i, aObject);
							bList.remove(j);
							break;
						}
						if (j == bList.size() - 1) {
							if (op.equals("*") || op.equals("/")) {
								throw new UnsupportedOperationException("指标维度值不能一一对应时,是不能做*/运算");
							}
						}
					}
				}
				if (op.equals("*") || op.equals("/")) {
					a.setData(list);
					return a;
				} else {
					// 如果是符号-,减数变相反数
					if (op.equals("-")) {
						for (int i = 0; i < bList.size(); i++) {
							JSONObject bObject = bList.get(i);
							bObject.put(b.getFields().iterator().next(), bObject.getBigDecimal(b.getFields().iterator().next()).negate());
							bList.set(i, bObject);
						}
					}
					list.addAll(bList);
					a.setData(list);
					return a;
				}

			}
		}

		DataCommonResponse calculateValue = new DataCommonResponse();
		return calculateValue;
	}

	/**
	 * 判断两个数维度值是不是相等
	 * 
	 * @param a          第一个数
	 * @param b          第二个数
	 * @param dimensions 维度的名称
	 * @return
	 */
	public static boolean dimensionValueEqual(JSONObject a, JSONObject b, Set<String> dimensions) {
		for (String dimension : dimensions) {
			if (!a.getString(dimension).equals(b.getString(dimension))) {
				return false;
			}
		}
		return true;
	}

	/**
	 * n+1 都有维度的数据相加
	 * 
	 * @param a 数组并且有维度
	 * @param b 1个数有维度
	 * @return
	 */
	public static DataCommonResponse addN1(DataCommonResponse a, DataCommonResponse b) {
		List<JSONObject> list = JSONObject.parseArray(a.getData().toString(), JSONObject.class);
		JSONObject bObject = JSONObject.parseObject(b.getData().toString());
		for (int i = 0; i < list.size(); i++) {
			JSONObject aObject = list.get(i);
			if (dimensionValueEqual(aObject, bObject, a.getDimensions())) {
				aObject.put(a.getFields().iterator().next(), aObject.getBigDecimal(a.getFields().iterator().next()).add(bObject.getBigDecimal(b.getFields().iterator().next())));
				list.set(i, aObject);
				a.setData(list);
				return a;
			}
		}
		list.add(bObject);
		a.setData(list);
		return a;
	}
}
