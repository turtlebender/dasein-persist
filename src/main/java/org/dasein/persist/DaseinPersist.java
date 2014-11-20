package org.dasein.persist;

import org.dasein.persist.annotations.Index;
import org.dasein.persist.annotations.IndexType;
import org.dasein.persist.annotations.Schema;
import org.dasein.util.CachedItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.TreeSet;

/**
 * Factory for creating and maintaining Persistent Caches.
 *
 * @author Tom Howe
 */
@SuppressWarnings("unused")
public class DaseinPersist {
    private final Logger logger = LoggerFactory.getLogger(DaseinPersist.class);
    private final HashMap<String, PersistentCache<? extends CachedItem>> caches = new HashMap<String, PersistentCache<? extends CachedItem>>();
    private final DataSource dataSource;

    public DaseinPersist(DataSource ds) {
        this.dataSource = ds;
    }

    public Transaction getTransaction(boolean readOnly) {
        return Transaction.getInstance(this.dataSource, readOnly);
    }

    public Transaction getTransaction(){
        return getTransaction(false);
    }

    public <T extends CachedItem> PersistentCache<T> getCache(Class<T> forClass) throws PersistenceException {
        SchemaMapper[] mappers = null;
        String schemaVersion = null;
        String entityName = null;

        for (Annotation annotation : forClass.getDeclaredAnnotations()) {
            if (annotation instanceof Schema) {
                schemaVersion = ((Schema) annotation).value();
                entityName = ((Schema) annotation).entity();

                Class<? extends SchemaMapper>[] mclasses = ((Schema) annotation).mappers();

                if (mclasses != null && mclasses.length > 0) {
                    mappers = new SchemaMapper[mclasses.length];
                    for (int i = 0; i < mclasses.length; i++) {
                        try {
                            mappers[i] = mclasses[i].newInstance();
                        } catch (Throwable t) {
                            throw new PersistenceException(t.getMessage());
                        }
                    }
                }
            }
        }
        return getCacheWithSchema(forClass, entityName, schemaVersion == null ? "0" : schemaVersion, mappers);
    }

    public <T extends CachedItem> PersistentCache<T> getCacheWithSchema(@Nonnull Class<T> forClass, @Nullable String entityName, @Nonnull String schemaVersion, @Nullable SchemaMapper... mappers) throws PersistenceException {
        Class<?> cls = forClass;

        while (!cls.getName().equals(Object.class.getName())) {
            for (Field field : cls.getDeclaredFields()) {
                for (Annotation annotation : field.getDeclaredAnnotations()) {
                    if (annotation instanceof Index) {
                        Index idx = (Index) annotation;

                        if (idx.type().equals(IndexType.PRIMARY)) {
                            return getCacheWithSchema(forClass, entityName, field.getName(), schemaVersion, mappers);
                        }
                    }
                }
            }
            cls = cls.getSuperclass();
        }
        throw new PersistenceException("No primary key field identified for: " + forClass.getName());
    }

    public <T extends CachedItem> PersistentCache<T> getCache(Class<T> forClass, String primaryKey) throws PersistenceException {
        SchemaMapper[] mappers = null;
        String schemaVersion = null;
        String entityName = null;

        for (Annotation annotation : forClass.getDeclaredAnnotations()) {
            if (annotation instanceof Schema) {
                schemaVersion = ((Schema) annotation).value();
                entityName = ((Schema) annotation).entity();

                Class<? extends SchemaMapper>[] mclasses = ((Schema) annotation).mappers();

                if (mclasses != null && mclasses.length > 0) {
                    mappers = new SchemaMapper[mclasses.length];
                    for (int i = 0; i < mclasses.length; i++) {
                        try {
                            mappers[i] = mclasses[i].newInstance();
                        } catch (Throwable t) {
                            throw new PersistenceException(t.getMessage());
                        }
                    }
                }
            }
        }
        return getCacheWithSchema(forClass, entityName, primaryKey, schemaVersion == null ? "0" : schemaVersion, mappers);
    }

    @SuppressWarnings("unchecked")
    public <T extends CachedItem> PersistentCache<T> getCacheWithSchema(@Nonnull Class<T> forClass, @Nullable String alternateEntytName, @Nonnull String primaryKey, @Nonnull String schemaVersion, @Nullable SchemaMapper... mappers) throws PersistenceException {
        PersistentCache<T> cache;
        String className = forClass.getName();

        synchronized (caches) {
            cache = (PersistentCache<T>) caches.get(className);
            if (cache != null) {
                return cache;
            }
        }

        TreeSet<Key> keys = new TreeSet<Key>();
        Class<?> cls = forClass;

        while (!cls.getName().equals(Object.class.getName())) {
            for (Field field : cls.getDeclaredFields()) {
                for (Annotation annotation : field.getDeclaredAnnotations()) {
                    if (annotation instanceof Index) {
                        Index idx = (Index) annotation;

                        if (idx.type().equals(IndexType.SECONDARY) || idx.type().equals(IndexType.FOREIGN)) {
                            String keyName = field.getName();

                            if (idx.multi() != null && idx.multi().length > 0) {
                                if (idx.cascade()) {
                                    int len = idx.multi().length;

                                    keys.add(new Key(keyName));
                                    for (int i = 0; i < len; i++) {
                                        String[] parts = new String[i + 2];

                                        parts[0] = keyName;
                                        for (int j = 0; j <= i; j++) {
                                            parts[j + 1] = idx.multi()[j];
                                        }
                                        keys.add(new Key(parts));
                                    }
                                } else {
                                    String[] parts = new String[idx.multi().length + 1];
                                    int i = 1;

                                    parts[0] = keyName;
                                    for (String name : idx.multi()) {
                                        parts[i++] = name;
                                    }
                                    Key k = new Key(parts);

                                    keys.add(k);
                                }
                            } else {
                                Key k;

                                if (idx.type().equals(IndexType.FOREIGN) && !idx.identifies().equals(CachedItem.class)) {
                                    k = new Key(idx.identifies(), keyName);
                                } else {
                                    k = new Key(keyName);
                                }
                                keys.add(k);
                            }
                        }
                    }
                }
            }
            cls = cls.getSuperclass();
        }
        cache = new RelationalCache<T>(this.dataSource);
        cache.initBase(forClass, alternateEntytName, schemaVersion, mappers, new Key(primaryKey), keys.toArray(new Key[keys.size()]));
        synchronized (caches) {
            PersistentCache<T> c = (PersistentCache<T>) caches.get(className);
            if (c != null) {
                cache = c;
            } else {
                caches.put(className, cache);
            }
        }
        return cache;
    }
}
