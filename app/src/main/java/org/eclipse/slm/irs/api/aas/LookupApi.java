package org.eclipse.slm.irs.api.aas;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import org.eclipse.digitaltwin.aas4j.v3.model.Result;
import org.eclipse.digitaltwin.aas4j.v3.model.SpecificAssetId;
import org.eclipse.slm.common.aas.model.discovery.AssetLink;
import org.eclipse.slm.common.aas.model.discovery.respones.GetAasIdsByAssetLinkResults;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@Validated
public interface LookupApi {

    @Operation(summary = "Deletes specified specific asset identifiers linked to an Asset Administration Shell: discovery via these specific asset IDs shall not be supported any longer", description = "", tags={ "Asset Administration Shell Basic Discovery API" })
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Specific asset identifiers deleted successfully"),

            @ApiResponse(responseCode = "404", description = "Not Found", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),

            @ApiResponse(responseCode = "200", description = "Default error handling for unmentioned status codes", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))) })
    @RequestMapping(value = "/lookup/shells/{aasIdentifier}",
            produces = { "application/json" },
            method = RequestMethod.DELETE)
    ResponseEntity<Void> deleteAllAssetLinksById(@Parameter(in = ParameterIn.PATH, description = "The Asset Administration Shell’s unique id (UTF8-BASE64-URL-encoded)", required=true, schema=@Schema()) @PathVariable("aasIdentifier") String aasIdentifier
    );


    @Operation(summary = "Returns a list of Asset Administration Shell IDs linked to specific asset identifiers or the global asset ID", description = "", tags={ "Asset Administration Shell Basic Discovery API" })
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Requested Asset Administration Shell IDs", content = @Content(mediaType = "application/json", schema = @Schema(implementation = GetAasIdsByAssetLinkResults.class))),

            @ApiResponse(responseCode = "200", description = "Default error handling for unmentioned status codes", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))) })
    @RequestMapping(value = "/lookup/shells",
            produces = { "application/json" },
            method = RequestMethod.GET)
    ResponseEntity<GetAasIdsByAssetLinkResults> getAllAssetAdministrationShellIdsByAssetLink(@Parameter(in = ParameterIn.QUERY, description = "A list of specific Asset identifiers. Each Asset identifier is a base64-url-encoded [SpecificAssetId](https://api.swaggerhub.com/domains/Plattform_i40/Part1-MetaModel-Schemas/V3.1.1#/components/schemas/SpecificAssetId)" ,schema=@Schema()) @Valid @RequestParam(value = "assetIds", required = false) List<String> assetIds
            , @Min(1)@Parameter(in = ParameterIn.QUERY, description = "The maximum number of elements in the response array" ,schema=@Schema(allowableValues={ "1" }, minimum="1"
            )) @Valid @RequestParam(value = "limit", required = false) Integer limit
            , @Parameter(in = ParameterIn.QUERY, description = "A server-generated identifier retrieved from pagingMetadata that specifies from which position the result listing should continue" ,schema=@Schema()) @Valid @RequestParam(value = "cursor", required = false) String cursor
    );


    @Operation(summary = "Returns a list of specific asset identifiers based on an Asset Administration Shell ID to edit discoverable content. The global asset ID is returned as specific asset ID with \"name\" equal to \"globalAssetId\" (see Constraint AASd-116).", description = "", tags={ "Asset Administration Shell Basic Discovery API" })
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Requested specific Asset identifiers (including the global asset ID represented by a specific asset ID)", content = @Content(mediaType = "application/json", array = @ArraySchema(schema = @Schema(implementation = SpecificAssetId.class)))),

            @ApiResponse(responseCode = "404", description = "Not Found", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),

            @ApiResponse(responseCode = "200", description = "Default error handling for unmentioned status codes", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))) })
    @RequestMapping(value = "/lookup/shells/{aasIdentifier}",
            produces = { "application/json" },
            method = RequestMethod.GET)
    ResponseEntity<List<SpecificAssetId>> getAllAssetLinksById(@Parameter(in = ParameterIn.PATH, description = "The Asset Administration Shell’s unique id (UTF8-BASE64-URL-encoded)", required=true, schema=@Schema()) @PathVariable("aasIdentifier") String aasIdentifier
    );


    @Operation(summary = "Creates or replaces all asset links associated to the Asset Administration Shell.", description = "", tags={ "Asset Administration Shell Basic Discovery API" })
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "Specific asset identifiers created successfully", content = @Content(mediaType = "application/json", array = @ArraySchema(schema = @Schema(implementation = SpecificAssetId.class)))),

            @ApiResponse(responseCode = "400", description = "Bad Request, e.g. the request parameters of the format of the request body is wrong.", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),

            @ApiResponse(responseCode = "404", description = "Not Found", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),

            @ApiResponse(responseCode = "409", description = "Conflict, a resource which shall be created exists already. Might be thrown if an object with the same id (for Identifiables) or idShort (for Referables within the same Container Element or Submodel) is contained in a POST request.", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),

            @ApiResponse(responseCode = "200", description = "Default error handling for unmentioned status codes", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))) })
    @RequestMapping(value = "/lookup/shells/{aasIdentifier}",
            produces = { "application/json" },
            consumes = { "application/json" },
            method = RequestMethod.POST)
    ResponseEntity<List<SpecificAssetId>> postAllAssetLinksById(@Parameter(in = ParameterIn.PATH, description = "The Asset Administration Shell’s unique id (UTF8-BASE64-URL-encoded)", required=true, schema=@Schema()) @PathVariable("aasIdentifier") String aasIdentifier
            , @Parameter(in = ParameterIn.DEFAULT, description = "A set of specific asset identifiers", required=true, schema=@Schema()) @Valid @RequestBody List<SpecificAssetId> body
    );


    @Operation(summary = "Returns a list of Asset Administration Shell IDs linked to specific asset identifiers or the global asset ID", description = "", tags={ "Asset Administration Shell Basic Discovery API" })
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Requested Asset Administration Shell IDs", content = @Content(mediaType = "application/json", schema = @Schema(implementation = GetAasIdsByAssetLinkResults.class))),

            @ApiResponse(responseCode = "400", description = "Bad Request, e.g. the request parameters of the format of the request body is wrong.", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))),

            @ApiResponse(responseCode = "200", description = "Default error handling for unmentioned status codes", content = @Content(mediaType = "application/json", schema = @Schema(implementation = Result.class))) })
    @RequestMapping(value = "/lookup/shellsByAssetLink",
            produces = { "application/json" },
            consumes = { "application/json" },
            method = RequestMethod.POST)
    ResponseEntity<GetAasIdsByAssetLinkResults> searchAllAssetAdministrationShellIdsByAssetLink(@Min(1)@Parameter(in = ParameterIn.QUERY, description = "The maximum number of elements in the response array" ,schema=@Schema(allowableValues={ "1" }, minimum="1"
                                                                                      )) @Valid @RequestParam(value = "limit", required = false) Integer limit
            , @Parameter(in = ParameterIn.QUERY, description = "A server-generated identifier retrieved from pagingMetadata that specifies from which position the result listing should continue" ,schema=@Schema()) @Valid @RequestParam(value = "cursor", required = false) String cursor
            , @Parameter(in = ParameterIn.DEFAULT, description = "A list of specific asset identifiers. Search for the global asset ID is supported by setting \"name\"  to \"globalAssetId\" (see Constraint AASd-116).", schema=@Schema()) @Valid @RequestBody List<AssetLink> body
    );

}

