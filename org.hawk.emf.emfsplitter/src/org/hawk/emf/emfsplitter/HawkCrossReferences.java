/*******************************************************************************
 * Copyright (c) 2016 University of York.
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

import org.eclipse.core.resources.IProject;
import org.eclipse.core.resources.ResourcesPlugin;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.emc.emf.EmfModel;
import org.eclipse.epsilon.eol.EolModule;
import org.eclipse.epsilon.eol.IEolModule;
import org.eclipse.epsilon.eol.dt.ExtensionPointToolNativeTypeDelegate;
import org.eclipse.epsilon.eol.exceptions.models.EolModelLoadingException;
import org.eclipse.epsilon.eol.execute.context.Variable;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.eol.models.IRelativePathResolver;
import org.hawk.core.IMetaModelResourceFactory;
import org.hawk.core.IModelIndexer;
import org.hawk.core.IStateListener.HawkState;
import org.hawk.core.IVcsManager;
import org.hawk.core.query.IQueryEngine;
import org.hawk.core.query.QueryExecutionException;
import org.hawk.core.runtime.LocalHawkFactory;
import org.hawk.emf.EMFPackage;
import org.hawk.emf.EMFWrapperFactory;
import org.hawk.emf.metamodel.EMFMetaModelResource;
import org.hawk.emf.metamodel.EMFMetaModelResourceFactory;
import org.hawk.emfresource.impl.LocalHawkResourceImpl;
import org.hawk.epsilon.emc.CEOLQueryEngine;
import org.hawk.epsilon.emc.EOLQueryEngine;
import org.hawk.epsilon.emc.wrappers.GraphNodeWrapper;
import org.hawk.orientdb.OrientDatabase;
import org.hawk.osgiserver.HModel;
import org.hawk.ui2.util.HUIManager;
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

				final IVcsManager repo = new Workspace();
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
				// TODO: use a path within the workspace directory?
				File fWorkspaceRoot = new File(ResourcesPlugin.getWorkspace().getRoot().getLocation().toString());
				final File storageFolder = new File(fWorkspaceRoot, "_emfsplitter-hawk");

				// TODO: limit plugins to EMF, use Neo4j if available
				hawkInstance = HModel.create(new LocalHawkFactory(), HAWK_INSTANCE, storageFolder,
							storageFolder.toURI().toASCIIString(), OrientDatabase.class.getName(), null, hawkManager,
							hawkManager.getCredentialsStore(), 0, 0);
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
		
	@Override
	public Object executeConstraint(String constraint, java.net.URI modelURI, boolean isUnit) {
			
		try {
			final HModel hawkInstance = getHawkInstance();

			//Create Engine to execute query
			final EOLQueryEngine q = new EOLQueryEngine();
			try {
				q.load(hawkInstance.getIndexer());
			} catch (EolModelLoadingException e) {
				throw new QueryExecutionException("Loading of EOLQueryEngine failed");
			}			
			
			final IEolModule module = new EolModule();
			
			module.getContext().getModelRepository().addModel(q);

			// Allows tools (e.g. EmfTool) registered through Eclipse extension points to work
			module.getContext().getNativeTypeDelegates().add(new ExtensionPointToolNativeTypeDelegate());

			module.parse(constraint);

			// isUnit = true  -> we only look at the object inside that specific file
			// isUnit = false -> we look at the objects inside the folder of that file and recursively down 

			final Map<String, Object> queryArguments = new HashMap<>();
			queryArguments.put("filePath", modelURI.toString());
			String repoURL;
			if (isUnit == true) {
				repoURL = modelURI.toString();
			} else {
				java.net.URI parent = modelURI.getPath().endsWith("/") ? modelURI.resolve("..") : modelURI.resolve(".");
				repoURL = parent.toString();
			}
			queryArguments.put("repoURL", repoURL);	
			addQueryArguments(queryArguments, module);	
			
			// Run the query
			Object result = module.execute();
			if (result instanceof List<?>) {
				replaceNodesWithURIs(result);
			}
			
			
			return result;
			
		} catch (Exception e) {
			HawkCrossReferencesPlugin.getDefault().logError(e);
			return new BasicEList<Object>();
		}		
	}

	@SuppressWarnings("unchecked")
	protected void replaceNodesWithURIs(Object result) {
		List<Object> l = (List<Object>)result;
		for (int i = 0; i < l.size(); i++) {
			Object elem = l.get(i);
			if (elem instanceof GraphNodeWrapper) {
				GraphNodeWrapper gw = (GraphNodeWrapper) elem;
				String url = gw.getNode().getProperty(IModelIndexer.IDENTIFIER_PROPERTY) + "";
				l.set(i, url);
			} else if (elem instanceof List<?>) {
				replaceNodesWithURIs(elem);
			}
		}
	}
	
	private void addQueryArguments(Map<String, Object> args, IEolModule module) {
		if (args != null) {
			for (Entry<String, Object> entry : args.entrySet()) {
				module.getContext().getFrameStack().putGlobal(new Variable(entry.getKey(), entry.getValue(), null));
			}
		}
	}
	
	private IModel getModel(String metamodelURI, String modelURI) {
		
		EmfModel emfModel = new EmfModel();
		StringProperties properties = new StringProperties();
		properties.put(EmfModel.PROPERTY_NAME, "resource");//Change Model to "resource"
		properties.put(EmfModel.PROPERTY_FILE_BASED_METAMODEL_URI,
				metamodelURI);
		properties.put(EmfModel.PROPERTY_MODEL_URI, 
				modelURI);
		properties.put(EmfModel.PROPERTY_READONLOAD, true + "");
		properties.put(EmfModel.PROPERTY_STOREONDISPOSAL, 
				true + "");
		try {
			emfModel.load(properties, (IRelativePathResolver) null);
		} catch (EolModelLoadingException e) {			
			e.printStackTrace();
		}
		return emfModel;	
	}

}
