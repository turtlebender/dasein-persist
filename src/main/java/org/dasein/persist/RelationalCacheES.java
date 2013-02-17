/**
 * Copyright (C) 1998-2011 enStratusNetworks LLC
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.persist;

import org.dasein.persist.jdbc.AutomatedSql.Operator;
import org.dasein.persist.jdbc.AutomatedSql.TranslationMethod;
import org.dasein.persist.jdbc.Counter;
import org.dasein.persist.jdbc.Creator;
import org.dasein.persist.jdbc.Deleter;
import org.dasein.persist.jdbc.Loader;
import org.dasein.persist.jdbc.Updater;
import org.dasein.util.CacheLoader;
import org.dasein.util.CacheManagementException;
import org.dasein.util.CachedItem;
import org.dasein.util.ConcurrentMultiCache;
import org.dasein.util.ConcurrentMultiEhcache;
import org.dasein.util.JitCollection;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorFilter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A Relational Cache backed by ConcurrentMultiEhcache
 * @param <T>
 */
public final class RelationalCacheES<T extends CachedItem> extends PersistentCache<T> {

    static public class OrderedColumn {
        public String  column;
        public boolean descending = false;
    }

    private String            readDataSource    = null;
    private TranslationMethod translationMethod = TranslationMethod.NONE;
    private String            writeDataSource   = null;

    public RelationalCacheES() { }

    /**
     * Constructs a new persistent factory for objects of the specified class with
     * the named unique identifier attributes.
     * @param cls the class of objects managed by this factory
     * @param keys a list of unique identifiers for instances of the specified class
     */
    protected void init(Class<T> cls, Key ... keys) {
        readDataSource = Execution.getDataSourceName(cls.getName(), true);
        writeDataSource = Execution.getDataSourceName(cls.getName(), false);
        if( readDataSource == null ) {
            readDataSource = writeDataSource;
        }
        if( writeDataSource == null ) {
            writeDataSource = readDataSource;
        }
    }

    public void setTranslationMethod(TranslationMethod translationMethod) {
    	this.translationMethod = translationMethod;
    }

    protected ConcurrentMultiCache<T> createCache(Class<T> cls, Key primary) {
        return new ConcurrentMultiEhcache<T>(cls, primary.getFields()[0]);
    }

    private Counter getCounter(SearchTerm[] whereTerms) {
        final SearchTerm[] terms = whereTerms;
        final RelationalCacheES<T> self = this;

        Counter counter = new Counter() {
            public void init() {
                setTarget(self.getEntityClassName());
                if( terms != null && terms.length > 0 ) {
                    ArrayList<Criterion> criteria = new ArrayList<Criterion>();

                    for( SearchTerm term : terms ) {
                        criteria.add(new Criterion(term.getColumn(), term.getOperator()));
                    }
                    setCriteria(criteria.toArray(new Criterion[criteria.size()]));
                }
            }

            public boolean isReadOnly() {
                return true;
            }
        };
        return counter;
    }

    private Creator getCreator() {
        final RelationalCacheES<T> self = this;

        Creator creator = new Creator() {
            public void init() {
                setTarget(self.getEntityClassName());
                switch (translationMethod) {
                    case CUSTOM: setCustomTranslating(); break;
                    case STANDARD: setTranslating(true); break;
                    case NONE: setTranslating(false); break;
                }
            }

            public boolean isReadOnly() {
                return false;
            }
        };
        return creator;
    }

    private Deleter getDeleter(SearchTerm ... terms) {
        final SearchTerm[] killTerms = terms;
        final RelationalCacheES<T> self = this;

        Deleter deleter = new Deleter() {
            public void init() {
                setTarget(self.getEntityClassName());
                if( killTerms != null && killTerms.length > 0 ) {
                    ArrayList<Criterion> criteria = new ArrayList<Criterion>();

                    for( SearchTerm term : killTerms ) {
                        criteria.add(new Criterion(term.getJoinEntity(), term.getColumn(), term.getOperator()));
                    }
                    setCriteria(criteria.toArray(new Criterion[criteria.size()]));
                }
                else {
                    setCriteria(self.getPrimaryKey().getFields());
                }
                switch (translationMethod) {
                    case CUSTOM: setCustomTranslating(); break;
                    case STANDARD: setTranslating(true); break;
                    case NONE: setTranslating(false); break;
                }

            }

            public boolean isReadOnly() {
                return false;
            }
        };
        return deleter;
    }

    private Loader getLoader(SearchTerm[] whereTerms, OrderedColumn[] orderBy) {
        final SearchTerm[] terms = whereTerms;
        final OrderedColumn[] order = orderBy;
        final RelationalCacheES<T> self = this;

        Loader loader = new Loader() {
            public void init() {
                setTarget(self.getEntityClassName());
                setEntityJoins(getJoins());
                if( terms != null && terms.length > 0 ) {
                    ArrayList<Criterion> criteria = new ArrayList<Criterion>();

                    for( SearchTerm term : terms ) {
                        criteria.add(new Criterion(term.getJoinEntity(), term.getColumn(), term.getOperator()));
                    }
                    setCriteria(criteria.toArray(new Criterion[criteria.size()]));
                }
                if( order != null && order.length > 0 ) {
                    ArrayList<String> cols = new ArrayList<String>();
                    boolean desc = order[0].descending;

                    for( OrderedColumn col : order ) {
                        cols.add(col.column);
                    }
                    setOrder(desc, cols.toArray(new String[cols.size()]));
                }
                switch (translationMethod) {
                    case CUSTOM: setCustomTranslating(); break;
                    case STANDARD: setTranslating(true); break;
                    case NONE: setTranslating(false); break;
                }
            }

            public boolean isReadOnly() {
                return true;
            }
        };
        return loader;
    }

    private Updater getUpdater() {
        final RelationalCacheES<T> self = this;

        Updater updater = new Updater() {
            public void init() {
                setTarget(self.getEntityClassName());
                setCriteria(self.getPrimaryKey().getFields());
                switch (translationMethod) {
                case CUSTOM: setCustomTranslating(); break;
                case STANDARD: setTranslating(true); break;
                case NONE: setTranslating(false); break;
                }
            }

            public boolean isReadOnly() {
                return false;
            }
        };
        return updater;
    }

    /**
     * Counts the total number of objects governed by this factory in the database.
     * @return the number of objects in the database
     * @throws org.dasein.persist.PersistenceException an error occurred counting the elements in the database
     */
    @Override
    public long count() throws PersistenceException {
        Transaction xaction = Transaction.getInstance(true);
        Counter counter = getCounter(null);

        try {
            Map<String,Object> results;
            long count;

            results = xaction.execute(counter, new HashMap<String,Object>(), readDataSource);
            count = ((Number)results.get("count")).longValue();
            xaction.commit();
            return count;
        }
        finally {
            xaction.rollback();
        }
    }

    @Override
    public long count(SearchTerm ... terms) throws PersistenceException {
        Transaction xaction = Transaction.getInstance(true);
        Counter counter = getCounter(terms);

        try {
            Map<String,Object> params = toParams(terms);
            Map<String,Object> results;
            long count;

            results = xaction.execute(counter, params, readDataSource);
            count = ((Number)results.get("count")).longValue();
            xaction.commit();
            return count;
        }
        finally {
            xaction.rollback();
        }
    }

    /**
     * Creates the specified object with the data provided in the specified state under
     * the governance of the specified transaction.
     * @param xaction the transaction governing this event
     * @param state the new state for the new object
     * @throws org.dasein.persist.PersistenceException an error occurred talking to the data store, or
     * creates are not supported
     */
    @Override
    public T create(Transaction xaction, Map<String,Object> state) throws PersistenceException {
        state.put("--key--", getPrimaryKey().getFields()[0]);
        xaction.execute(getCreator(), state, writeDataSource);
        return getCache().find(state);
    }

    @Nonnull
    @Override
    public Collection<T> find(@Nullable SearchTerm[] terms, JiteratorFilter<T> filter, Boolean orderDesc, String ... orderFields) throws PersistenceException {
        OrderedColumn[] order;

        if( orderFields == null ) {
            order = new OrderedColumn[0];
        }
        else {
            int i = 0;

            order = new OrderedColumn[orderFields.length];
            for( String field : orderFields ) {
                order[i] = new OrderedColumn();
                order[i].column = field;
                order[i].descending = (orderDesc != null && orderDesc);
                i++;
            }
        }
        return this.load(getLoader(terms, order), filter, toParams(terms));
    }

    /**
     * Retrieves the object uniquely identified by the value for the specified ID field.
     * @param primaryKeyValue the ID field identifying the object
     * @return the object matching the query criterion
     * @throws PersistenceException an error occurred talking to the data store
     */
    @Override
    public T get(final Object primaryKeyValue) throws PersistenceException {
        CacheLoader<T> loader = new CacheLoader<T>() {
            public T load(Object ... args) {
                try {
                    SearchTerm[] terms = new SearchTerm[1];
                    Collection<T> list;

                    terms[0] = new SearchTerm((String)args[0], Operator.EQUALS, args[1]);
                    try {
                        list = RelationalCacheES.this.load(getLoader(terms, null), null, toParams(terms));
                    } catch( PersistenceException e ) {
                        throw new RuntimeException(e);
                    }
                    if( list.isEmpty() ) {
                        return null;
                    }
                    return list.iterator().next();
                } catch (Throwable t) {
                    throw new RuntimeException(t);
                }
            }
        };
        try {
            return getCache().find(getPrimaryKeyField(), primaryKeyValue, loader, getPrimaryKeyField(), primaryKeyValue);
        } catch( CacheManagementException e ) {
            throw new PersistenceException(e);
        } catch( RuntimeException e ) {
            Throwable t = e.getCause();
            if( t != null && t instanceof PersistenceException ) {
                throw (PersistenceException)t;
            }
            throw new PersistenceException(e);
        }
    }

    @Override
    public String getSchema() throws PersistenceException {
        StringBuilder schema = new StringBuilder();

        schema.append("CREATE TABLE ");
        schema.append(getSqlNameForClassName(getEntityClassName()));
        schema.append(" (");

        schema.append(");");
        return schema.toString();
    }

    protected String getSqlName(String nom) {
        StringBuilder sql = new StringBuilder();

        for(int i=0; i<nom.length(); i++) {
            char c = nom.charAt(i);

            if( Character.isLetter(c) && !Character.isLowerCase(c) ) {
                if( i != 0 ) {
                    sql.append("_");
                }
                sql.append(Character.toLowerCase(c));
            }
            else {
                sql.append(c);
            }
        }
        return sql.toString();
    }

    protected String getSqlNameForClassName(String cname) {
        String[] parts = cname.split("\\.");
        int i;

        if( parts != null && parts.length > 1 ) {
            cname = parts[parts.length-1];
        }
        i = cname.lastIndexOf("$");
        if( i != -1 ) {
            cname = cname.substring(i+1);
        }
        return getSqlName(cname);
    }

    /**
     * Loads all elements of this class from the data store. Use this method only when you know
     * exactly what you are doing. Otherwise, you will pull a lot of data.
     * @return all objects from the database
     * @throws org.dasein.persist.PersistenceException an error occurred executing the query
     */
    @Override
    public Collection<T> list() throws PersistenceException {
        return find(null, null, false);
    }

    private Map<String,Object> toParams(SearchTerm ... searchTerms) {
        HashMap<String,Object> params = new HashMap<String,Object>();

        if( searchTerms != null ) {
            for( SearchTerm term : searchTerms ) {
                params.put(term.getColumn(), term.getValue());
            }
        }
        return params;
    }

    @SuppressWarnings("unchecked")
    private Collection<T> load(Loader loader, JiteratorFilter<T> filter, Map<String,Object> params) throws PersistenceException {
        Transaction xaction = Transaction.getInstance(true);
        final Jiterator<T> it = new Jiterator<T>(filter);

        params.put("--key--", getPrimaryKey().getFields()[0]);
        try {
            final Map<String,Object> results;

            results = xaction.execute(loader, params, readDataSource);
            xaction.commit();

            // not a separate thread so that the reentrant lock will let the find() through
            try {
                for( Map<String,Object> map: (Collection<Map<String,Object>>)results.get(Loader.LISTING) ) {
                    for( String fieldName : map.keySet() ) {
                        LookupDelegate delegate = getLookupDelegate(fieldName);

                        if( delegate != null && !delegate.validate((String)map.get(fieldName)) ) {
                            throw new PersistenceException("Unable to validate " + fieldName + " value of " + map.get(fieldName));
                        }
                    }
                    it.push(getCache().find(map));
                }
                it.complete();
            }
            catch( Exception e ) {
                it.setLoadException(e);
            }
            catch( Throwable t ) {
                it.setLoadException(new RuntimeException(t));
            }
            return new JitCollection<T>(it, getEntityClassName());
        }
        catch( PersistenceException e ) {
            it.setLoadException(e);
            throw e;
        }
        catch( RuntimeException e ) {
            it.setLoadException(e);
            throw e;
        }
        catch( Throwable t ) {
            RuntimeException e = new RuntimeException(t);

            it.setLoadException(e);
            throw e;
        }
        finally {
            xaction.rollback();
        }
    }

    /**
     * Removes the specified item from the system permanently.
     * @param xaction the transaction under which this event is occurring
     * @param item the item to be removed
     * @throws org.dasein.persist.PersistenceException an error occurred talking to the data store or
     * removal of these objects is prohibited
     */
    @Override
    public void remove(Transaction xaction, T item) throws PersistenceException {
        xaction.execute(getDeleter(), getCache().getKeys(item), writeDataSource);
        getCache().release(item);
    }

    /**
     * Removes the specified item from the system permanently.
     * Warning: this method is not safe to use without manual cache reasoning/adjustment.
     * @param xaction the transaction under which this event is occurring
     * @param terms the criteria to use for removal
     * @throws PersistenceException
     */
    @Override
    public void remove(Transaction xaction, SearchTerm ... terms) throws PersistenceException {
        xaction.execute(getDeleter(terms), toParams(terms), writeDataSource);
    }

    public String toString() {
        return getCache().toString();
    }

    /**
     * Updates the specified object with the data provided in the specified state under
     * the governance of the specified transaction.
     * @param xaction the transaction governing this event
     * @param item the item to be updated
     * @param state the new state for the updated object
     * @throws org.dasein.persist.PersistenceException an error occurred talking to the data store, or
     * updates are not supported
     */
    @Override
    public void update(Transaction xaction, T item, Map<String,Object> state) throws PersistenceException {     
        state.put("--key--", getPrimaryKey().getFields()[0]);
        xaction.execute(getUpdater(), state, writeDataSource);
        // important to update the cache to maintain read-after-update consistency because of
        // the way ConcurrentMultiEhcache loading locks are implemented
        getCache().cache(item);
    }    
}
