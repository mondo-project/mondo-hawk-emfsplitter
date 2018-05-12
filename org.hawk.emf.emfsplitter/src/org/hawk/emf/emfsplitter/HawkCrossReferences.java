/*******************************************************************************
 * Copyright (c) 2016-2018 University of York, Aston University.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Antonio Garcia-Dominguez - initial API and implementation
 *******************************************************************************/
package org.hawk.emf.emfsplitter;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.eclipse.core.resources.IFile;
import org.eclipse.core.resources.IProject;
import org.eclipse.core.resources.IWorkspaceRoot;
import org.eclipse.core.resources.ResourcesPlugin;
import org.eclipse.core.runtime.Path;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EcorePackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.epsilon.eol.EolModule;
import org.eclipse.epsilon.eol.IEolModule;
import org.eclipse.epsilon.eol.exceptions.models.EolModelLoadingException;
import org.eclipse.epsilon.eol.execute.context.Variable;
import org.hawk.core.IMetaModelResourceFactory;
import org.hawk.core.IModelIndexer;
import org.hawk.core.IStateListener.HawkState;
import org.hawk.core.IVcsManager;
import org.hawk.core.graph.IGraphDatabase;
import org.hawk.core.graph.IGraphTransaction;
import org.hawk.core.query.IQueryEngine;
import org.hawk.core.query.QueryExecutionException;
import org.hawk.core.runtime.LocalHawkFactory;
import org.hawk.emf.EMFPackage;
import org.hawk.emf.EMFWrapperFactory;
import org.hawk.emf.metamodel.EMFMetaModelResource;
import org.hawk.emf.metamodel.EMFMetaModelResourceFactory;
import org.hawk.emf.model.EMFModelResourceFactory;
import org.hawk.emfresource.impl.LocalHawkResourceImpl;
import org.hawk.epsilon.emc.CEOLQueryEngine;
import org.hawk.epsilon.emc.EOLQueryEngine;
import org.hawk.epsilon.emc.wrappers.GraphNodeWrapper;
import org.hawk.graph.ModelElementNode;
import org.hawk.neo4j_v2.Neo4JDatabase;
import org.hawk.osgiserver.HModel;
import org.hawk.ui2.util.HUIManager;
import org.hawk.workspace.LocalHistoryWorkspace;
import org.hawk.workspace.Workspace;
import org.mondo.generate.index.project.ext.IIndexAttribute;
import org.mondo.modular.constraint.ext.def.IExecuteConstraint;
import org.mondo.modular.references.ext.IEditorCrossReferences;


/**
 * Integrates Hawk into the cross reference selector dialog for EMF-Splitter. At
 * the moment, it indexes the whole workspace into a local Hawk instance, which
 * is directly accessed through the underlying graph.
 */
public class HawkCrossReferences implements IEditorCrossReferences, IIndexAttribute, IExecuteConstraint {

	private static final String HAWK_INSTANCE = "emfsplitter";

	@Override
	public boolean init(List<String> metamodelURIs, String modularNature) {
		try {
			final HModel hm = getHawkInstance();

			metamodelURIs = new ArrayList<>(metamodelURIs);
			metamodelURIs.add(EcorePackage.eINSTANCE.getNsURI());

			if (!hm.getRegisteredMetamodels().containsAll(metamodelURIs)) {
				final List<File> dumped = new ArrayList<>();

				try {
					final IMetaModelResourceFactory emfFactory = hm.getIndexer()
							.getMetaModelParser(EMFMetaModelResourceFactory.class.getName());

					for (String metamodelURI : metamodelURIs) {
						if (hm.getRegisteredMetamodels().contains(metamodelURI)) {
							continue;
						}

						final EPackage epkg = EPackage.Registry.INSTANCE.getEPackage(metamodelURI);
						if (epkg == null) {
							throw new NoSuchElementException(
									String.format("No metamodel with URL '%s' is available in the global EMF registry.",
											metamodelURI));
						}

						final EMFWrapperFactory wf = new EMFWrapperFactory();
						final String pkgEcore = emfFactory.dumpPackageToString(
								new EMFPackage(epkg, wf, new EMFMetaModelResource(epkg.eResource(), wf, emfFactory)));
						final File tmpEcore = File.createTempFile("tmp", ".ecore");
						try (final FileOutputStream fOS = new FileOutputStream(tmpEcore)) {
							fOS.write(pkgEcore.getBytes());
						}
						dumped.add(tmpEcore);
					}

					hm.getIndexer().registerMetamodels(dumped.toArray(new File[dumped.size()]));
				} finally {
					for (File tmpEcore : dumped) {
						if (tmpEcore.exists()) {
							tmpEcore.delete();
						}
					}
				}

				final IVcsManager repo = new LocalHistoryWorkspace();
				hm.addVCS(repo.getLocation(), repo.getClass().getName(), "", "", false);
				hm.sync();
			}
		} catch (Exception e) {
			HawkCrossReferencesPlugin.getDefault().logError(e);
			return false;
		}
		return true;
	}

	@Override
	public boolean finish(String modularNature) {
		// Nothing to do - Hawk shuts down by itself using a workbench listener
		return true;
	}

	@Override
	public boolean isGlobal() {
		// The Hawk Workspace VCS doesn't allow filtering projects yet
		return true;
	}

	@Override
	public boolean isNature(String modularNature) {
		// The Hawk Workspace VCS doesn't allow filtering projects yet
		return true;
	}

	@Override
	public EList<?> getChoicesOfValues(final String modularNature, final Resource res, final boolean searchAll, final EClass anEClass, final String eolFilter) {
		LocalHawkResourceImpl hawkResource = null;
		try {
			final HModel hawkInstance = getHawkInstance();

			// Look for the Hawk resource in the resource set first
			for (Resource r : res.getResourceSet().getResources()) {
				if (r instanceof LocalHawkResourceImpl) {
					hawkResource = (LocalHawkResourceImpl) r;
					if (hawkResource.getIndexer().getName().equals(HAWK_INSTANCE)) {
						// This is the EMF-Splitter local Hawk resource
						break;
					}
				}
			}

			// If it's not in the resource set, load it and add it to the resource set
			if (hawkResource == null) {
				hawkResource = new LocalHawkResourceImpl(URI.createURI("hawk://"), hawkInstance.getIndexer(), true, Arrays.asList("*"), Arrays.asList("*"));
				res.getResourceSet().getResources().add(hawkResource);
				hawkResource.load(null);
			}

			// Construct the select condition
			String selectCondition = eolFilter;
			final Map<String, Object> queryArguments = new HashMap<>();
			if (!searchAll) {
				if (selectCondition != null) {
					selectCondition += " and ";
				} else {
					selectCondition = "";
				}

				final String repoURL = new Workspace().getLocation();
				String filePath = res.getURI().toString();
				if (filePath.startsWith(repoURL)) {
					filePath = filePath.substring(repoURL.length());
				}
				queryArguments.put("repoURL", repoURL);
				queryArguments.put("filePath", filePath);

				selectCondition += "self.isContainedWithin(repoURL, filePath)";
			}

			// Construct the full EOL query
			String query = String.format("return `%s`::`%s`.all", anEClass.getEPackage().getNsURI(), anEClass.getName());
			if (selectCondition != null) {
				query += String.format(".select(self|%s)", selectCondition);
			}
			query += ";";

			// Run the query
			final Map<String, Object> context = new HashMap<>();
			context.put(IQueryEngine.PROPERTY_ARGUMENTS, queryArguments);
			context.put(IQueryEngine.PROPERTY_FILECONTEXT, "*");
			context.put(IQueryEngine.PROPERTY_REPOSITORYCONTEXT, "*");
			EList<EObject> instances = hawkResource.fetchByQuery(EOLQueryEngine.TYPE, query, context);

			// Optionally, filter by project nature
			if (modularNature != null) {
				final List<String> acceptedPrefixes = new ArrayList<>();
				for (IProject project : ResourcesPlugin.getWorkspace().getRoot().getProjects()) {
					if (project.isOpen() && project.getNature(modularNature) != null) {
						String prefix = URI.createPlatformResourceURI(project.getFullPath().toString(), false).path() + "/";
						acceptedPrefixes.add(prefix);
					}
				}

				filterByNature:
				for (Iterator<EObject> itInstance = instances.iterator(); itInstance.hasNext(); ) {
					final EObject eob = itInstance.next();
					for (String prefix : acceptedPrefixes) {
						final String path = eob.eResource().getURI().path();
						if (path.startsWith(prefix)) {
							continue filterByNature;
						}
					}
					itInstance.remove();
				}
			}

			return instances;
		} catch (Exception e) {
			HawkCrossReferencesPlugin.getDefault().logError(e);
			return new BasicEList<Object>();
		}
	}

	/**
	 * Returns the Hawk instance that indexes the whole workspace, ensuring that
	 * it exists and that it is in the {@link HawkState#RUNNING} state.
	 */
	protected HModel getHawkInstance() throws Exception {
		final HUIManager hawkManager = HUIManager.getInstance();
		synchronized (hawkManager) {
			HModel hawkInstance = hawkManager.getHawkByName(HAWK_INSTANCE);
			if (hawkInstance == null) {
				File fWorkspaceRoot = new File(ResourcesPlugin.getWorkspace().getRoot().getLocation().toString());
				final File storageFolder = new File(fWorkspaceRoot, "_emfsplitter-hawk");

				// Limit to EMF, EOL and core graph updater
				List<String> plugins = Arrays.asList(
					EMFMetaModelResourceFactory.class.getName(),
					EMFModelResourceFactory.class.getName()
				);

				// No periodic updates 
				hawkInstance = HModel.create(new LocalHawkFactory(), HAWK_INSTANCE, storageFolder,
							storageFolder.toURI().toASCIIString(), Neo4JDatabase.class.getName(),
							plugins, hawkManager, hawkManager.getCredentialsStore(), 0, 0);
			}

			if (!hawkInstance.isRunning()) {
				hawkInstance.start(hawkManager);
				hawkInstance.getIndexer().waitFor(HawkState.UPDATING, 3000);
				hawkInstance.getIndexer().waitFor(HawkState.RUNNING);
			}
			return hawkInstance;
		}
	}

	@Override
	public boolean addIndexedAttribute(String nsURI, String type, String attribute) {
		try {
			final HModel hawkInstance = getHawkInstance();
			hawkInstance.addIndexedAttribute(nsURI, type, attribute);
			return true;
		} catch (Exception ex) {
			HawkCrossReferencesPlugin.getDefault().logError(ex);
			return false;
		}
	}

	private static class ConstraintExecutor {
		private IModelIndexer indexer;
		private IGraphDatabase db;
		private CEOLQueryEngine eol;

		public void setIndexer(IModelIndexer indexer) {
			if (indexer != this.indexer) {
				this.indexer = indexer;
				db = null;
				eol = null;
			}
		}

		public void setGraph(IGraphDatabase db) {
			if (db != this.db) {
				this.db = db;
				eol = null;
			}
		}

		public CEOLQueryEngine getQueryEngine() throws QueryExecutionException {
			if (eol == null) {
				eol = new CEOLQueryEngine();
				try {
					eol.load(indexer);
				} catch (EolModelLoadingException e) {
					throw new QueryExecutionException("Loading of EOLQueryEngine failed");
				}
			}
			return eol;
		}

		public Object execute(String constraint, java.net.URI metaModelURI, List<String> metamodelURIs, boolean isUnit,	IFile[] modelFile) throws Exception {
			final IEolModule module = new EolModule();
			final CEOLQueryEngine queryEngine = getQueryEngine();
			module.getContext().getModelRepository().addModel(queryEngine);
			module.parse(constraint);

			// Model.getAllOf(...) expects:
			// * The URI of the ePackage
			// * The name of the eClass
			// * String with comma-separated list of patterns (based on paths within
			// workspace starting with slash, e.g. /project/folder/b.xmi)
			// - specific files: /project/folder/b.xmi
			// - wildcards: /project/* (everything in that project),
			// /project/folder/* (everything in that folder),
			// /project/folder/b*.xmi (every .xmi starting with b in that folder)

			// Convert java.net.URI to workspace path (substring after platform:/resource)
			String filePath = modelFile[0].getFullPath().toString();
			final Map<String, Object> queryArguments = new HashMap<>();
			queryArguments.put("filePath", filePath);
			queryArguments.put("mmURI", metamodelURIs.get(0));

			// isUnit = true -> we only look at the object inside that specific file
			// isUnit = false -> we look at the objects inside the folder of that file and
			// recursively down
			String repoURL;
			if (isUnit == true) {
				repoURL = modelFile[0].getFullPath().toString();
			} else {
				repoURL = modelFile[0].getParent().getFullPath().toString().concat("/*");
			}

			Map<String, Object> context = new HashMap<String, Object>();
			final String defaultNamespaces = buildDefaultNamespaces(metamodelURIs);

			context.put(EOLQueryEngine.PROPERTY_REPOSITORYCONTEXT, Workspace.REPOSITORY_URL);
			context.put(EOLQueryEngine.PROPERTY_DEFAULTNAMESPACES, defaultNamespaces);
			if (isUnit) {
				context.put(EOLQueryEngine.PROPERTY_FILECONTEXT, repoURL);
				context.put(EOLQueryEngine.PROPERTY_FILEFIRST, isUnit + "");
			} else {
				context.put(EOLQueryEngine.PROPERTY_SUBTREECONTEXT, filePath);
				context.put(EOLQueryEngine.PROPERTY_SUBTREE_DERIVEDALLOF, "true");
			}
			queryEngine.setContext(context);
			addQueryArguments(queryArguments, module);

			try (IGraphTransaction tx = db.beginTransaction()) {
				Object result = module.execute();
				if (result instanceof List<?>) {
					replaceNodesWithURIs(result);
				}
				tx.success();
				return result;
			}
		}

		private void addQueryArguments(Map<String, Object> args, IEolModule module) {
			if (args != null) {
				for (Entry<String, Object> entry : args.entrySet()) {
					module.getContext().getFrameStack().putGlobal(new Variable(entry.getKey(), entry.getValue(), null));
				}
			}
		}

		protected String buildDefaultNamespaces(List<String> metamodelURIs) {
			StringBuilder sb = new StringBuilder();
			boolean first = true;
			for (String mmURI : metamodelURIs) {
				if (first) {
					first = false;
				} else {
					sb.append(",");
				}
				sb.append(mmURI);
			}
			return sb.toString();
		}

		@SuppressWarnings("unchecked")
		protected void replaceNodesWithURIs(Object result) {
			List<Object> l = (List<Object>)result;
			for (int i = 0; i < l.size(); i++) {
				Object elem = l.get(i);
				if (elem instanceof GraphNodeWrapper) {
					final GraphNodeWrapper gw = (GraphNodeWrapper) elem;
					final ModelElementNode meNode = new ModelElementNode(gw.getNode());

					IWorkspaceRoot workspaceRoot = ResourcesPlugin.getWorkspace().getRoot();
					IFile iFile = workspaceRoot.getFile(new Path(meNode.getFileNode().getFilePath()));
					if (iFile != null) {
						l.set(i, iFile.getLocationURI().toString() + "#" + meNode.getElementId());
					}
				} else if (elem instanceof List<?>) {
					replaceNodesWithURIs(elem);
				}
			}
		}
		
	}

	// Share a bit of state between calls, so we can cache some repeated calls
	static ConstraintExecutor constraintExecutor = new ConstraintExecutor();

	@Override
	public Object executeConstraint(final String constraint, final java.net.URI modelURI, final java.net.URI metaModelURI, final List<String> metamodelURIs, final boolean isUnit) {
		try {
			if (constraintExecutor == null) {
				constraintExecutor = new ConstraintExecutor();
			}
			final HModel hawkInstance = getHawkInstance();

			// We need to do it here rather than in the scheduled task, or it will fail to find any files
			final IWorkspaceRoot workspaceRoot = ResourcesPlugin.getWorkspace().getRoot();
			final IFile[] modelFile = workspaceRoot.findFilesForLocationURI(modelURI);

			/*
			 * We schedule this task to be done as soon as possible, while avoiding
			 * interference with normal indexing of Hawk. The building process seems to
			 * trigger change notifications at the start, which trigger reindexing. This is
			 * a problem for backends like Neo4j, which forbid queries during batch updates.
			 */
			ScheduledFuture<Object> scheduled = hawkInstance.getIndexer().scheduleTask(new Callable<Object>() {
				@Override
				public Object call() throws Exception {
					constraintExecutor.setIndexer(hawkInstance.getIndexer());
					constraintExecutor.setGraph(hawkInstance.getGraph());
					return constraintExecutor.execute(constraint, metaModelURI, metamodelURIs, isUnit, modelFile);
				}
			}, 0);

			return scheduled.get();		
		} catch (Exception e) {
			HawkCrossReferencesPlugin.getDefault().logError(e);
			return new BasicEList<Object>();
		}
	}

	@Override
	public void update() {
		try {
			final IModelIndexer indexer = getHawkInstance().getIndexer();
			waitForImmediateSync(indexer, new Callable<Object>() {
				@Override
				public Object call() throws Exception {
					return null;
				}
			});
		} catch (Throwable e) {
			HawkCrossReferencesPlugin.getDefault().logError(e);
		}
	}

	protected void waitForImmediateSync(final IModelIndexer indexer, final Callable<?> r) throws Throwable {
		final Semaphore sem = new Semaphore(0);
		final SyncEndListener changeListener = new SyncEndListener(r, sem);
		indexer.addGraphChangeListener(changeListener);
		indexer.requestImmediateSync();
		if (!sem.tryAcquire(6000, TimeUnit.SECONDS)) {
			throw new TimeoutException();
		} else {
			indexer.removeGraphChangeListener(changeListener);
			if (changeListener.getThrowable() != null) {
				throw changeListener.getThrowable();
			}
		}
	}

}
