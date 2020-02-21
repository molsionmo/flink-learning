package com.zhisheng.libraries.cep.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc:
 * Created by zhisheng on 2019/10/29 上午10:34
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Data
public class SubEvent extends Event{
    private Integer volume;

    public SubEvent(Integer id,String name, Integer volume){
        super(id, name);
        this.volume = volume;
    }
}
