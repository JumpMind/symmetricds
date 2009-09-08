/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Keith Naas <knaas@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.util.List;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager.ReservationType;

/**
 * Configured within symmetric-web.xml
 */
public class NodeConcurrencyFilter extends AbstractFilter {

    private final static ILog log = LogFactory.getLog(NodeConcurrencyFilter.class);

    private IConcurrentConnectionManager concurrentConnectionManager;

    private IConfigurationService configurationService;

    private String reservationUriPattern;

    @Override
    public boolean isContainerCompatible() {
        return true;
    }

    public void doFilter(final ServletRequest req, final ServletResponse resp, final FilterChain chain)
            throws IOException, ServletException {

        HttpServletRequest httpRequest = (HttpServletRequest) req;
        String poolId = httpRequest.getRequestURI();
        String nodeId = StringUtils.trimToNull(req.getParameter(WebConstants.NODE_ID));
        String method = httpRequest.getMethod();

        if (method.equals("HEAD") && matchesUriPattern(normalizeRequestUri(httpRequest), reservationUriPattern)) {
            // I read here:
            // http://java.sun.com/j2se/1.5.0/docs/guide/net/http-keepalive.html
            // that keepalive likes to have a known content length. I also read
            // that HEAD is better if no content is going to be returned.
            resp.setContentLength(0);
            if (!concurrentConnectionManager.reserveConnection(nodeId, poolId, ReservationType.SOFT)) {
                sendError(resp, HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            } else {
              //  buildSuspendIgnoreResponseHeaders(nodeId, resp);
            }
        } else if (concurrentConnectionManager.reserveConnection(nodeId, poolId, ReservationType.HARD)) {
            try {
                //buildSuspendIgnoreResponseHeaders(nodeId, resp);
                chain.doFilter(req, resp);
            } finally {
                concurrentConnectionManager.releaseConnection(nodeId, poolId);
            }
        } else {
            sendError(resp, HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }
    }

    private void buildSuspendIgnoreResponseHeaders(final String nodeId, final ServletResponse resp) {
        HttpServletResponse httpResponse = (HttpServletResponse) resp;

        StringBuffer suspendChannelsBuffer = new StringBuffer();
        StringBuffer ignoreChannelsBuffer = new StringBuffer();

        List<NodeChannel> ncs = configurationService.getChannels(nodeId);

        for (NodeChannel nc : ncs) {
            if (nc.isSuspended()) {
                suspendChannelsBuffer.append(',').append(nc.getId());
            }
            if (nc.isIgnored()) {
                ignoreChannelsBuffer.append(',').append(nc.getId());
            }
        }

        String suspendChannels = StringUtils.trimToNull(suspendChannelsBuffer.toString());
        String ignoreChannels = StringUtils.trimToNull(ignoreChannelsBuffer.toString());

        if (suspendChannels != null) {
            httpResponse.setHeader(WebConstants.SUSPENDED_CHANNELS, suspendChannels.substring(1));
        }

        if (ignoreChannels != null) {
            httpResponse.setHeader(WebConstants.IGNORED_CHANNELS, ignoreChannels.substring(1));
        }

    }

    @Override
    protected ILog getLog() {
        return log;
    }

    public void setConcurrentConnectionManager(IConcurrentConnectionManager concurrentConnectionManager) {
        this.concurrentConnectionManager = concurrentConnectionManager;
    }

    public void setReservationUriPattern(String reservationUriPattern) {
        this.reservationUriPattern = reservationUriPattern;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public IConfigurationService getConfigurationService() {
        return configurationService;
    }
}
