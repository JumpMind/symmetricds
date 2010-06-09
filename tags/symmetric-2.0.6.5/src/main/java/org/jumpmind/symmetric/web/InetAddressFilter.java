/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Dave Michels <dmichels2@users.sourceforge.net>,
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
import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.transport.InetAddressResourceHandler;

/**
 * This better be the first filter that executes!
 */
public class InetAddressFilter extends AbstractTransportFilter<InetAddressResourceHandler> 
    implements IBuiltInExtensionPoint {
    
    public static final String INET_ADDRESS_FILTERS = "inetAddressFilters";

    public static final String INET_ADDRESS_ALLOW_MULICAST = "inetAddressAllowMultcast";

    private static final ILog log = LogFactory.getLog(InetAddressFilter.class);

    private InetAddressResourceHandler authorizer;

    @Override
    public void init(final FilterConfig config) throws ServletException {
        super.init(config);
        authorizer = getTransportResourceHandler();
        final String addressFilters = config.getInitParameter(INET_ADDRESS_FILTERS);
        if (addressFilters != null) {
            try {
                authorizer.setAddressFilters(addressFilters);
            } catch (final UnknownHostException e) {
                throw new ServletException("Invalid fddress filter string: " + addressFilters, e);
            }
        }

        final String multicastAllowed = config.getInitParameter(INET_ADDRESS_ALLOW_MULICAST);
        if (!StringUtils.isBlank(multicastAllowed)) {
            authorizer.setMulicastAllowed(Boolean.parseBoolean(multicastAllowed.trim()));
        }
    }

    @Override
    public boolean isContainerCompatible() {
        return true;
    }

    public void doFilter(final ServletRequest req, final ServletResponse resp, final FilterChain chain)
            throws IOException, ServletException {
        final HttpServletRequest httpRequest = (HttpServletRequest) req;
        final String sourceAddrString = httpRequest.getRemoteAddr();
        try {
            final InetAddress sourceAddr = InetAddress.getByName(sourceAddrString);
            log.debug("AddressAuthorizing", sourceAddr.toString());
            if (authorizer.isAuthorized(sourceAddr)) {
                chain.doFilter(req, resp);
            } else {
                log.info("AddressDenied", sourceAddr.toString());
                sendError(resp, HttpServletResponse.SC_FORBIDDEN);
            }
        } catch (final UnknownHostException uhe) {
            sendError(resp, HttpServletResponse.SC_FORBIDDEN);
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        authorizer.clearFilters();
    }

    @Override
    protected ILog getLog() {
        return log;
    }
}
