/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
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
package org.jumpmind.symmetric.config;

import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;

/**
 * An adapter for the trigger listener interface so you need only implement the
 * methods you are interested in.
 */
public class TriggerCreationAdapter implements ITriggerCreationListener {

    public void tableDoesNotExist(Trigger trigger) {
    }

    public void triggerCreated(Trigger trigger, TriggerHistory history) {
    }

    public void triggerFailed(Trigger trigger, Exception ex) {
    }

    public void triggerInactivated(Trigger trigger, TriggerHistory oldHistory) {
    }

    public boolean isAutoRegister() {
        return true;
    }

}
