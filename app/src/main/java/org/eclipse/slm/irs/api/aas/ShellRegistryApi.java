package org.eclipse.slm.irs.api.aas;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.eclipse.digitaltwin.aas4j.v3.model.AssetAdministrationShellDescriptor;
import org.eclipse.digitaltwin.aas4j.v3.model.AssetKind;
import org.eclipse.digitaltwin.aas4j.v3.model.Result;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelDescriptor;
import org.eclipse.slm.aas.model.shellregistry.respones.GetAssetAdministrationShellDescriptorsResult;
import org.eclipse.slm.aas.model.shellregistry.respones.GetSubmodelDescriptorsResult;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

// Version: V3.0.3_SSP-001
// https://app.swaggerhub.com/apis/Plattform_i40/AssetAdministrationShellRegistryServiceSpecification/V3.0.3_SSP-001

@Validated
public interface ShellRegistryApi {

    @Operation(summary = "Deletes an Asset Administration Shell Descriptor, i.e. de-registers an AAS", description = "", tags={ "Asset Administration Shell Registry API" })
    @ApiResponses(value = { 
        @ApiResponse(responseCode = "204", description = "Asset Administration Shell Descriptor deleted successfully"),
        
        @ApiResponse(responseCode = "400", description = "Bad Request, e.g. the request parameters of the format of the request body is wrong.", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "404", description = "Not Found", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "500", description = "Internal Server Error", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "200", description = "Default error handling for unmentioned status codes", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))) })
    @RequestMapping(value = "/shell-descriptors/{aasIdentifier}",
        produces = { "application/json" }, 
        method = RequestMethod.DELETE)
    ResponseEntity<Void> deleteAssetAdministrationShellDescriptorById(@Parameter(in = ParameterIn.PATH, description = "The Asset Administration Shell’s unique id (UTF8-BASE64-URL-encoded)", required=true, schema=@Schema()) @PathVariable("aasIdentifier") String aasIdentifierEncoded
);


    @Operation(summary = "Deletes a Submodel Descriptor, i.e. de-registers a submodel", description = "", tags={ "Asset Administration Shell Registry API" })
    @ApiResponses(value = { 
        @ApiResponse(responseCode = "204", description = "Submodel Descriptor deleted successfully"),
        
        @ApiResponse(responseCode = "400", description = "Bad Request, e.g. the request parameters of the format of the request body is wrong.", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "404", description = "Not Found", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "500", description = "Internal Server Error", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "200", description = "Default error handling for unmentioned status codes", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))) })
    @RequestMapping(value = "/shell-descriptors/{aasIdentifier}/submodel-descriptors/{submodelIdentifier}",
        produces = { "application/json" }, 
        method = RequestMethod.DELETE)
    ResponseEntity<Void> deleteSubmodelDescriptorByIdThroughSuperpath(@Parameter(in = ParameterIn.PATH, description = "The Asset Administration Shell’s unique id (UTF8-BASE64-URL-encoded)", required=true, schema=@Schema()) @PathVariable("aasIdentifier") String aasIdentifierEncoded
, @Parameter(in = ParameterIn.PATH, description = "The Submodel’s unique id (UTF8-BASE64-URL-encoded)", required=true, schema=@Schema()) @PathVariable("submodelIdentifier") String submodelIdentifier
);


    @Operation(summary = "Returns all Asset Administration Shell Descriptors", description = "", tags={ "Asset Administration Shell Registry API" })
    @ApiResponses(value = { 
        @ApiResponse(responseCode = "200", description = "Requested Asset Administration Shell Descriptors", content = @Content(mediaType = "application/json", schema = @Schema(implementation = GetAssetAdministrationShellDescriptorsResult.class))),
        
        @ApiResponse(responseCode = "400", description = "Bad Request, e.g. the request parameters of the format of the request body is wrong.", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "403", description = "Forbidden", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "500", description = "Internal Server Error", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "200", description = "Default error handling for unmentioned status codes", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))) })
    @RequestMapping(value = "/shell-descriptors",
        produces = { "application/json" }, 
        method = RequestMethod.GET)
    ResponseEntity<GetAssetAdministrationShellDescriptorsResult> getAllAssetAdministrationShellDescriptors(@Min(1)@Parameter(in = ParameterIn.QUERY, description = "The maximum number of elements in the response array" ,schema=@Schema(allowableValues={ "1" }, minimum="1"
)) @Valid @RequestParam(value = "limit", required = false) Integer limit
, @Parameter(in = ParameterIn.QUERY, description = "A server-generated identifier retrieved from pagingMetadata that specifies from which position the result listing should continue" ,schema=@Schema()) @Valid @RequestParam(value = "cursor", required = false) String cursor
, @Parameter(in = ParameterIn.QUERY, description = "The Asset's kind (Instance or Type)" ,schema=@Schema()) @Valid @RequestParam(value = "assetKind", required = false) AssetKind assetKind
, @Pattern(regexp="^([\\x09\\x0a\\x0d\\x20-\\ud7ff\\ue000-\\ufffd]|\\ud800[\\udc00-\\udfff]|[\\ud801-\\udbfe][\\udc00-\\udfff]|\\udbff[\\udc00-\\udfff])*$") @Size(min=1,max=2000) @Parameter(in = ParameterIn.QUERY, description = "The Asset's type (UTF8-BASE64-URL-encoded)" ,schema=@Schema()) @Valid @RequestParam(value = "assetType", required = false) String assetType
);


    @Operation(summary = "Returns all Submodel Descriptors", description = "", tags={ "Asset Administration Shell Registry API" })
    @ApiResponses(value = { 
        @ApiResponse(responseCode = "200", description = "Requested Submodel Descriptors", content = @Content(mediaType = "application/json", schema = @Schema(implementation = GetSubmodelDescriptorsResult.class))),
        
        @ApiResponse(responseCode = "400", description = "Bad Request, e.g. the request parameters of the format of the request body is wrong.", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "403", description = "Forbidden", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "404", description = "Not Found", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "500", description = "Internal Server Error", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "200", description = "Default error handling for unmentioned status codes", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))) })
    @RequestMapping(value = "/shell-descriptors/{aasIdentifier}/submodel-descriptors",
        produces = { "application/json" }, 
        method = RequestMethod.GET)
    ResponseEntity<GetSubmodelDescriptorsResult> getAllSubmodelDescriptorsThroughSuperpath(@Parameter(in = ParameterIn.PATH, description = "The Asset Administration Shell’s unique id (UTF8-BASE64-URL-encoded)", required=true, schema=@Schema()) @PathVariable("aasIdentifier") String aasIdentifierEncoded
, @Min(1)@Parameter(in = ParameterIn.QUERY, description = "The maximum number of elements in the response array" ,schema=@Schema(allowableValues={ "1" }, minimum="1"
)) @Valid @RequestParam(value = "limit", required = false) Integer limit
, @Parameter(in = ParameterIn.QUERY, description = "A server-generated identifier retrieved from pagingMetadata that specifies from which position the result listing should continue" ,schema=@Schema()) @Valid @RequestParam(value = "cursor", required = false) String cursor
);


    @Operation(summary = "Returns a specific Asset Administration Shell Descriptor", description = "", tags={ "Asset Administration Shell Registry API" })
    @ApiResponses(value = { 
        @ApiResponse(responseCode = "200", description = "Requested Asset Administration Shell Descriptor", content = @Content(mediaType = "application/json", schema = @Schema(implementation = AssetAdministrationShellDescriptor.class))),
        
        @ApiResponse(responseCode = "400", description = "Bad Request, e.g. the request parameters of the format of the request body is wrong.", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "403", description = "Forbidden", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "404", description = "Not Found", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "500", description = "Internal Server Error", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "200", description = "Default error handling for unmentioned status codes", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))) })
    @RequestMapping(value = "/shell-descriptors/{aasIdentifier}",
        produces = { "application/json" }, 
        method = RequestMethod.GET)
    ResponseEntity<AssetAdministrationShellDescriptor> getAssetAdministrationShellDescriptorById(@Parameter(in = ParameterIn.PATH, description = "The Asset Administration Shell’s unique id (UTF8-BASE64-URL-encoded)", required=true, schema=@Schema()) @PathVariable("aasIdentifier") String aasIdentifierEncoded
);


    @Operation(summary = "Returns a specific Submodel Descriptor", description = "", tags={ "Asset Administration Shell Registry API" })
    @ApiResponses(value = { 
        @ApiResponse(responseCode = "200", description = "Requested Submodel Descriptor", content = @Content(mediaType = "application/json", schema = @Schema(implementation = SubmodelDescriptor.class))),
        
        @ApiResponse(responseCode = "400", description = "Bad Request, e.g. the request parameters of the format of the request body is wrong.", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "403", description = "Forbidden", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "404", description = "Not Found", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "500", description = "Internal Server Error", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "200", description = "Default error handling for unmentioned status codes", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))) })
    @RequestMapping(value = "/shell-descriptors/{aasIdentifier}/submodel-descriptors/{submodelIdentifier}",
        produces = { "application/json" }, 
        method = RequestMethod.GET)
    ResponseEntity<SubmodelDescriptor> getSubmodelDescriptorByIdThroughSuperpath(@Parameter(in = ParameterIn.PATH, description = "The Asset Administration Shell’s unique id (UTF8-BASE64-URL-encoded)", required=true, schema=@Schema()) @PathVariable("aasIdentifier") String aasIdentifierEncoded
, @Parameter(in = ParameterIn.PATH, description = "The Submodel’s unique id (UTF8-BASE64-URL-encoded)", required=true, schema=@Schema()) @PathVariable("submodelIdentifier") String submodelIdentifier
);


    @Operation(summary = "Creates a new Asset Administration Shell Descriptor, i.e. registers an AAS", description = "", tags={ "Asset Administration Shell Registry API" })
    @ApiResponses(value = { 
        @ApiResponse(responseCode = "201", description = "Asset Administration Shell Descriptor created successfully", content = @Content(mediaType = "application/json", schema = @Schema(implementation = AssetAdministrationShellDescriptor.class))),
        
        @ApiResponse(responseCode = "400", description = "Bad Request, e.g. the request parameters of the format of the request body is wrong.", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "403", description = "Forbidden", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "409", description = "Conflict, a resource which shall be created exists already. Might be thrown if a Submodel or SubmodelElement with the same ShortId is contained in a POST request.", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "500", description = "Internal Server Error", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "200", description = "Default error handling for unmentioned status codes", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))) })
    @RequestMapping(value = "/shell-descriptors",
        produces = { "application/json" }, 
        consumes = { "application/json" }, 
        method = RequestMethod.POST)
    ResponseEntity<AssetAdministrationShellDescriptor> postAssetAdministrationShellDescriptor(@Parameter(in = ParameterIn.DEFAULT, description = "Asset Administration Shell Descriptor object", required=true, schema=@Schema()) @Valid @RequestBody AssetAdministrationShellDescriptor body
);


    @Operation(summary = "Creates a new Submodel Descriptor, i.e. registers a submodel", description = "", tags={ "Asset Administration Shell Registry API" })
    @ApiResponses(value = { 
        @ApiResponse(responseCode = "201", description = "Submodel Descriptor created successfully", content = @Content(mediaType = "application/json", schema = @Schema(implementation = SubmodelDescriptor.class))),
        
        @ApiResponse(responseCode = "400", description = "Bad Request, e.g. the request parameters of the format of the request body is wrong.", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "403", description = "Forbidden", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "404", description = "Not Found", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "409", description = "Conflict, a resource which shall be created exists already. Might be thrown if a Submodel or SubmodelElement with the same ShortId is contained in a POST request.", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "500", description = "Internal Server Error", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "200", description = "Default error handling for unmentioned status codes", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))) })
    @RequestMapping(value = "/shell-descriptors/{aasIdentifier}/submodel-descriptors",
        produces = { "application/json" }, 
        consumes = { "application/json" }, 
        method = RequestMethod.POST)
    ResponseEntity<SubmodelDescriptor> postSubmodelDescriptorThroughSuperpath(@Parameter(in = ParameterIn.PATH, description = "The Asset Administration Shell’s unique id (UTF8-BASE64-URL-encoded)", required=true, schema=@Schema()) @PathVariable("aasIdentifier") String aasIdentifierEncoded
, @Parameter(in = ParameterIn.DEFAULT, description = "Submodel Descriptor object", required=true, schema=@Schema()) @Valid @RequestBody SubmodelDescriptor body
);


    @Operation(summary = "Updates an existing Asset Administration Shell Descriptor", description = "", tags={ "Asset Administration Shell Registry API" })
    @ApiResponses(value = { 
        @ApiResponse(responseCode = "204", description = "Asset Administration Shell Descriptor updated successfully"),
        
        @ApiResponse(responseCode = "400", description = "Bad Request, e.g. the request parameters of the format of the request body is wrong.", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "403", description = "Forbidden", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "404", description = "Not Found", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "500", description = "Internal Server Error", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "200", description = "Default error handling for unmentioned status codes", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))) })
    @RequestMapping(value = "/shell-descriptors/{aasIdentifier}",
        produces = { "application/json" }, 
        consumes = { "application/json" }, 
        method = RequestMethod.PUT)
    ResponseEntity<Void> putAssetAdministrationShellDescriptorById(@Parameter(in = ParameterIn.PATH, description = "The Asset Administration Shell’s unique id (UTF8-BASE64-URL-encoded)", required=true, schema=@Schema()) @PathVariable("aasIdentifier") String aasIdentifierEncoded
, @Parameter(in = ParameterIn.DEFAULT, description = "Asset Administration Shell Descriptor object", required=true, schema=@Schema()) @Valid @RequestBody AssetAdministrationShellDescriptor body
);


    @Operation(summary = "Updates an existing Submodel Descriptor", description = "", tags={ "Asset Administration Shell Registry API" })
    @ApiResponses(value = { 
        @ApiResponse(responseCode = "204", description = "Submodel Descriptor updated successfully"),
        
        @ApiResponse(responseCode = "400", description = "Bad Request, e.g. the request parameters of the format of the request body is wrong.", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "403", description = "Forbidden", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "404", description = "Not Found", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "500", description = "Internal Server Error", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),
        
        @ApiResponse(responseCode = "200", description = "Default error handling for unmentioned status codes", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))) })
    @RequestMapping(value = "/shell-descriptors/{aasIdentifier}/submodel-descriptors/{submodelIdentifier}",
        produces = { "application/json" }, 
        consumes = { "application/json" }, 
        method = RequestMethod.PUT)
    ResponseEntity<Void> putSubmodelDescriptorByIdThroughSuperpath(@Parameter(in = ParameterIn.PATH, description = "The Asset Administration Shell’s unique id (UTF8-BASE64-URL-encoded)", required=true, schema=@Schema()) @PathVariable("aasIdentifier") String aasIdentifierEncoded
, @Parameter(in = ParameterIn.PATH, description = "The Submodel’s unique id (UTF8-BASE64-URL-encoded)", required=true, schema=@Schema()) @PathVariable("submodelIdentifier") String submodelIdentifier
, @Parameter(in = ParameterIn.DEFAULT, description = "Submodel Descriptor object", required=true, schema=@Schema()) @Valid @RequestBody SubmodelDescriptor body
);

}

