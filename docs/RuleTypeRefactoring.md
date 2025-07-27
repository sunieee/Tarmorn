# 规则类型统一重构文档

## 概述

本次重构将规则学习框架中的规则类型表示方式从多个布尔变量（`mineParamZero`, `mineParamCyclic`, `mineParamAcyclic`）和一个长度变量（`mineParamLength`）统一为单个 `Int` 类型，大幅简化了代码复杂度。

## 重构内容

### 1. Dice.kt 改进

#### 新增常量定义
```kotlin
// Rule type constants for unified Int representation
const val RULE_TYPE_ZERO = 0
const val RULE_TYPE_CYCLIC_1 = 1
const val RULE_TYPE_CYCLIC_2 = 2
// ... 继续到 RULE_TYPE_CYCLIC_10 = 10
const val RULE_TYPE_ACYCLIC_1 = 11
const val RULE_TYPE_ACYCLIC_2 = 12
const val RULE_TYPE_ACYCLIC_3 = 13
```

#### 新增便利方法
- `createCyclicType(length: Int): Int` - 创建循环规则类型
- `createAcyclicType(length: Int): Int` - 创建非循环规则类型
- `getRuleTypeName(ruleType: Int): String` - 获取规则类型名称
- `isValidRuleType(ruleType: Int): Boolean` - 验证规则类型有效性

### 2. Scorer.kt 重构

#### 简化字段定义
```kotlin
// 旧版本：4个变量
private var mineParamCyclic = true
private var mineParamAcyclic = false
private var mineParamZero = false
private var mineParamLength = 1

// 新版本：1个变量
private var ruleType = Dice.RULE_TYPE_CYCLIC_1
```

#### 新的参数设置方法
```kotlin
// 新的主要方法
fun setSearchParameters(ruleType: Int) {
    require(Dice.isValidRuleType(ruleType)) { "Invalid rule type: $ruleType" }
    this.ruleType = ruleType
    // ... 其他逻辑
}

// 保持向后兼容的废弃方法
@Deprecated("Use setSearchParameters(ruleType: Int) instead")
fun setSearchParameters(zero: Boolean, cyclic: Boolean, acyclic: Boolean, len: Int)
```

#### 简化规则挖掘逻辑
```kotlin
// 旧版本：多个条件判断
if (mineParamZero) { /* 零规则挖掘 */ }
if (mineParamCyclic) { /* 循环规则挖掘 */ }
if (mineParamAcyclic) { /* 非循环规则挖掘 */ }

// 新版本：统一解码
val isZero = Dice.decodedDiceZero(ruleType)
val isCyclic = Dice.decodedDiceCyclic(ruleType)
val isAcyclic = Dice.decodedDiceAcyclic(ruleType)
val length = Dice.decodedDiceLength(ruleType)

if (isZero) { /* 零规则挖掘 */ }
if (isCyclic) { /* 循环规则挖掘 */ }
if (isAcyclic) { /* 非循环规则挖掘 */ }
```

### 3. Learn.kt 简化

#### 调用方式简化
```kotlin
// 旧版本：多步解码
val type = dice!!.ask(0)
val zero = Dice.decodedDiceZero(type)
val cyclic = Dice.decodedDiceCyclic(type)
val acyclic = Dice.decodedDiceAcyclic(type)
val len = Dice.decodedDiceLength(type)
s.setSearchParameters(zero, cyclic, acyclic, len)

// 新版本：直接传递
val ruleType = dice!!.ask(0)
s.setSearchParameters(ruleType)
```

## 优势总结

### 1. 代码简洁性
- **字段数量减少**: 从4个字段减少到1个字段
- **参数传递简化**: 从5个参数减少到1个参数
- **逻辑清晰**: 统一的类型表示减少了条件判断的复杂性

### 2. 类型安全
- **编译时检查**: 使用常量定义避免魔数
- **运行时验证**: `isValidRuleType()` 方法确保类型有效性
- **错误处理**: `require()` 语句提供清晰的错误信息

### 3. 可维护性
- **集中管理**: 所有规则类型定义集中在 `Dice` 类中
- **向后兼容**: 保留废弃方法确保平滑迁移
- **文档完善**: 常量和方法都有清晰的文档说明

### 4. 扩展性
- **易于添加新类型**: 只需在 `Dice` 中添加新常量
- **统一接口**: 所有规则类型使用相同的 API
- **便利方法**: 提供创建和验证方法

## 使用示例

```kotlin
// 使用常量
scorer.setSearchParameters(Dice.RULE_TYPE_CYCLIC_3)

// 动态创建
val cyclicType = Dice.createCyclicType(5)
scorer.setSearchParameters(cyclicType)

// 获取类型信息
val typeName = Dice.getRuleTypeName(Dice.RULE_TYPE_ACYCLIC_2)
println("Type name: $typeName") // 输出: Type name: Acyclic-2
```

## 迁移指南

1. **立即生效**: 新的 API 可以立即使用
2. **渐进迁移**: 旧的 API 标记为废弃但仍然可用
3. **完全替代**: 建议在下个版本中完全移除废弃方法

这次重构显著提高了代码的可读性和可维护性，同时保持了向后兼容性，是一次成功的架构改进。
