package com.xiaohongshu.db.hercules.core.option;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 配置options，例如各个option名以及帮助命令行option的建立
 * 继承完全通过组合来做，子类自觉final
 */
public abstract class BaseOptionsConf {

    public static final String HELP = "help";

    private List<SingleOptionConf> optionConfList;

    /**
     * 组合模式
     */
    private List<BaseOptionsConf> ancestorList;

    /**
     * 检查套娃有没有套回自己（导致无穷递归）
     * 后代检查时不用担心祖先有问题，因为祖先在new的时候必然已经检查过了
     *
     * @param self
     */
    private static void validateAncestor(BaseOptionsConf self) {
        Set<BaseOptionsConf> checkedSet = new HashSet<>();
        Queue<BaseOptionsConf> uncheckedQueue = new LinkedList<>();
        uncheckedQueue.offer(self);
        while (uncheckedQueue.size() > 0) {
            BaseOptionsConf tmp = uncheckedQueue.poll();
            // 如果已经check过了就不必再检查一遍
            if (checkedSet.contains(tmp)) {
                continue;
            }
            checkedSet.add(tmp);
            for (BaseOptionsConf tmpAncestor : tmp.getAncestorList()) {
                // 不允许套娃套到自己
                if (tmpAncestor.equals(self)) {
                    throw new RuntimeException(String.format("The [%s]'s ancestor [%s] has a [%s] as an ancestor.",
                            self.getClass().getCanonicalName(),
                            tmp.getClass().getCanonicalName(),
                            self.getClass().getCanonicalName()));
                }
                uncheckedQueue.offer(tmpAncestor);
            }
        }
    }

    public BaseOptionsConf() {
        // 禁止继承，继承会把套娃搞得更复杂
        if (this.getClass().getSuperclass() != BaseOptionsConf.class) {
            throw new RuntimeException(String.format("The %s should extends from %s, instead of %s.",
                    this.getClass().getCanonicalName(),
                    BaseOptionsConf.class.getCanonicalName(),
                    this.getClass().getSuperclass().getCanonicalName()));
        }
        ancestorList = generateAncestorList();
        ancestorList = ancestorList == null ? Collections.EMPTY_LIST : ancestorList;
        validateAncestor(this);
        optionConfList = generateOptionConf();
    }

    abstract protected List<BaseOptionsConf> generateAncestorList();

    public List<BaseOptionsConf> getAncestorList() {
        return ancestorList;
    }

    /**
     * 配置参数列表
     * 如果祖先中有相同param名的配置后来的会覆盖前面的配置
     *
     * @return
     */
    private List<SingleOptionConf> generateOptionConf() {
        List<SingleOptionConf> res = new LinkedList<>();
        for (BaseOptionsConf ancestor : ancestorList) {
            res.addAll(ancestor.generateOptionConf());
        }
        List<SingleOptionConf> thisRes = innerGenerateOptionConf();
        res.addAll(thisRes == null ? new ArrayList<>(0) : thisRes);
        return res;
    }

    /**
     * 配置参数列表
     *
     * @return
     */
    abstract protected List<SingleOptionConf> innerGenerateOptionConf();

    public void validateOptions(GenericOptions options) {
        for (BaseOptionsConf ancestor : ancestorList) {
            ancestor.validateOptions(options);
        }
        innerValidateOptions(options);
    }

    /**
     * 检查options，包括类型、参数间依赖关系等一切需要检查的东西，遇错直接抛
     * 由于validate没有覆盖规则，祖先后代的会全部执行，强烈建议祖先的约束松于后代，不然会有些options被误杀（后代允许，祖先不允许）
     *
     * @param options
     */
    abstract public void innerValidateOptions(GenericOptions options);

    public void processOptions(GenericOptions options) {
        for (BaseOptionsConf ancestor : ancestorList) {
            ancestor.processOptions(options);
        }
        innerProcessOptions(options);
    }

    /**
     * 处理一些options的变更/生成/合并，如用户自定义的column-type到hercules的type转换+拷贝
     *
     * @param options
     */
    public void innerProcessOptions(GenericOptions options) {
    }

    protected final List<SingleOptionConf> clearOption(List<SingleOptionConf> confList, String confName) {
        return confList.stream()
                .filter(conf -> !conf.getName().equals(confName))
                .collect(Collectors.toList());
    }

    public Map<String, SingleOptionConf> getOptionsMap() {
        Map<String, SingleOptionConf> optionConfMap = new HashMap<>();
        for (SingleOptionConf conf : optionConfList) {
            optionConfMap.put(conf.getName(), conf);
        }
        return optionConfMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode();
    }
}
