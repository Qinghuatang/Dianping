package com.hmdp.utils;

import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import org.springframework.beans.BeanUtils;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class LoginInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {

        // 1.获取session
        HttpSession session = request.getSession();

        // 2.获取session中的用户信息
        UserDTO userDTO = (UserDTO) session.getAttribute("user");

        // 3.判断用户是否存在
        if (userDTO == null){
            // 4.不存在则拦截，返回401状态码
            response.setStatus(401);
            return false;   // 不放行
        }

        // 5.存在，保存用户信息到 ThreadLocal
        UserHolder.saveUser(userDTO);

        // 6.放行
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        HandlerInterceptor.super.afterCompletion(request, response, handler, ex);
    }
}
